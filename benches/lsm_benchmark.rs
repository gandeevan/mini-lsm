use criterion::{criterion_group, criterion_main, Criterion};
use mini_lsm::DB;

pub fn lsm_benchmark_small_values(c: &mut Criterion) {
    let mut kvstore = DB::new("/tmp/log.txt").expect("Failed to create a new DB");
    let mut group = c.benchmark_group("lsm-benchmarks");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("insert_or_update", |b| {
        b.iter(|| {
            let key: Vec<u8> = (0..64).map(|_| rand::random::<u8>()).collect();
            let value: Vec<u8> = (0..1024).map(|_| rand::random::<u8>()).collect();
            kvstore
                .insert_or_update(&key, &value)
                .expect("Insert failed")
        })
    });
    group.finish();
}

criterion_group!(benches, lsm_benchmark_small_values);
criterion_main!(benches);
