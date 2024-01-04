
use criterion::{criterion_group, criterion_main, Criterion};
use mini_lsm::DB;
use rand::random;

pub fn lsm_benchmark(c: &mut Criterion) {
    let mut kvstore = DB::<i32, i32>::new();
    let mut group = c.benchmark_group("lsm-benchmarks");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function(
        "insert_or_update", 
        |b| {
            b.iter(|| kvstore.insert_or_update(random(), random()).expect(
                "Insert failed"
            ))
        } 
    );
    group.finish();
}

criterion_group!(benches, lsm_benchmark);
criterion_main!(benches);