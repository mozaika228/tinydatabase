use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tinydatabase::{BatchOp, Database};

fn unique_dir(tag: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("tinydatabase-bench-{tag}-{stamp}"))
}

fn make_kv(i: usize) -> (Vec<u8>, Vec<u8>) {
    (
        format!("key-{i:08}").into_bytes(),
        format!("value-{i:08}").into_bytes(),
    )
}

fn tinydb_put_bench(c: &mut Criterion) {
    c.bench_function("tinydb_put_10k", |b| {
        b.iter_batched(
            || {
                let dir = unique_dir("tiny-put");
                let db = Database::open(&dir).expect("open db");
                (db, dir)
            },
            |(db, dir)| {
                for i in 0..10_000 {
                    let (k, v) = make_kv(i);
                    db.set(&k, &v).expect("set");
                }
                let _ = fs::remove_dir_all(dir);
            },
            BatchSize::SmallInput,
        )
    });
}

fn tinydb_get_bench(c: &mut Criterion) {
    c.bench_function("tinydb_get_10k", |b| {
        b.iter_batched(
            || {
                let dir = unique_dir("tiny-get");
                let db = Database::open(&dir).expect("open db");
                for i in 0..10_000 {
                    let (k, v) = make_kv(i);
                    db.set(&k, &v).expect("seed set");
                }
                (db, dir)
            },
            |(db, dir)| {
                for i in 0..10_000 {
                    let (k, _) = make_kv(i);
                    let _ = db.get(&k).expect("get");
                }
                let _ = fs::remove_dir_all(dir);
            },
            BatchSize::SmallInput,
        )
    });
}

fn tinydb_batch_put_bench(c: &mut Criterion) {
    c.bench_function("tinydb_write_batch_10k", |b| {
        b.iter_batched(
            || {
                let dir = unique_dir("tiny-batch");
                let db = Database::open(&dir).expect("open db");
                (db, dir)
            },
            |(db, dir)| {
                let mut ops = Vec::with_capacity(10_000);
                for i in 0..10_000 {
                    let (k, v) = make_kv(i);
                    ops.push(BatchOp::Put(k, v));
                }
                db.write_batch(&ops).expect("write_batch");
                let _ = fs::remove_dir_all(dir);
            },
            BatchSize::SmallInput,
        )
    });
}

fn raw_append_file_put_bench(c: &mut Criterion) {
    c.bench_function("raw_append_file_put_10k", |b| {
        b.iter_batched(
            || {
                let dir = unique_dir("raw-file");
                fs::create_dir_all(&dir).expect("mkdir");
                let path = dir.join("kv.log");
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .expect("open");
                (file, dir)
            },
            |(mut file, dir)| {
                for i in 0..10_000 {
                    let (k, v) = make_kv(i);
                    file.write_all(&(k.len() as u32).to_le_bytes())
                        .expect("write k len");
                    file.write_all(&(v.len() as u32).to_le_bytes())
                        .expect("write v len");
                    file.write_all(&k).expect("write k");
                    file.write_all(&v).expect("write v");
                }
                file.sync_data().expect("sync");
                let _ = fs::remove_dir_all(dir);
            },
            BatchSize::SmallInput,
        )
    });
}

fn in_memory_btreemap_bench(c: &mut Criterion) {
    c.bench_function("in_memory_btreemap_put_10k", |b| {
        b.iter(|| {
            let mut map = BTreeMap::new();
            for i in 0..10_000 {
                let (k, v) = make_kv(i);
                map.insert(k, v);
            }
        })
    });
}

criterion_group!(
    kv_benches,
    tinydb_put_bench,
    tinydb_get_bench,
    tinydb_batch_put_bench,
    raw_append_file_put_bench,
    in_memory_btreemap_bench
);
criterion_main!(kv_benches);
