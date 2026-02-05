use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

// Note: This benchmark should be in rust-core/src/bin/ or use rust-core as a dependency
// For now, we'll use a simplified version that can be compiled separately

fn create_test_shard(path: &PathBuf, num_samples: usize, sample_size: usize) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    let mut buf = Vec::new();

    // ヘッダー
    let header_offset = 0;
    let metadata_offset = ShardHeader::SIZE as u64;
    let data_offset = metadata_offset + 10000; // メタデータ用のスペース
    let header = ShardHeader::new(metadata_offset, data_offset);
    header.write(&mut buf)?;

    // メタデータ
    let mut samples = Vec::new();
    let mut current_offset = 0u64;
    for _ in 0..num_samples {
        samples.push(SampleMetadata {
            offset: current_offset,
            size: sample_size as u64,
        });
        current_offset += sample_size as u64;
    }

    let metadata = ShardMetadata {
        num_samples: num_samples as u64,
        samples,
    };

    let metadata_start = buf.len();
    metadata.write(&mut buf)?;
    let metadata_end = buf.len();

    // データオフセットを更新
    let data_offset = metadata_end as u64;
    buf[header_offset + 4 + 2..header_offset + 4 + 2 + 8]
        .copy_from_slice(&data_offset.to_le_bytes());

    // パディング
    while buf.len() < data_offset as usize {
        buf.push(0);
    }

    // サンプルデータを書き込む
    let sample_data = vec![0u8; sample_size];
    for _ in 0..num_samples {
        buf.extend_from_slice(&sample_data);
    }

    file.write_all(&buf)?;
    file.flush()?;
    Ok(())
}

fn benchmark_standard_read(path: &PathBuf, num_samples: usize, sample_size: usize) -> Duration {
    let mut file = File::open(path).unwrap();
    let mut total_time = Duration::ZERO;

    for _ in 0..num_samples {
        let start = Instant::now();
        let mut buf = vec![0u8; sample_size];
        file.read_exact(&mut buf).unwrap();
        total_time += start.elapsed();
    }

    total_time
}

fn benchmark_mmap_read(path: &PathBuf, num_samples: usize, sample_size: usize) -> Duration {
    use memmap2::MmapOptions;
    use std::fs::File;

    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let mut total_time = Duration::ZERO;

    // データセクションの開始位置を仮定（実際にはヘッダーを読む必要がある）
    let data_start = 1000; // 簡略化

    for i in 0..num_samples {
        let start = Instant::now();
        let offset = data_start + i * sample_size;
        let _slice = &mmap[offset..offset + sample_size];
        total_time += start.elapsed();
    }

    total_time
}

fn benchmark_zero_copy_loader(shard_paths: &[PathBuf], num_samples: usize) -> Duration {
    let loader = DataLoader::new(shard_paths).unwrap();
    let start = Instant::now();

    for i in 0..num_samples {
        let _sample = loader.get_sample(i).unwrap();
    }

    start.elapsed()
}

fn main() {
    println!("Zero-Copy Loader Benchmark");
    println!("==========================\n");

    let num_shards = 4;
    let samples_per_shard = 1000;
    let sample_size = 1024 * 1024; // 1MB per sample
    let total_samples = num_shards * samples_per_shard;

    // テストデータを作成
    let temp_dir = std::env::temp_dir();
    let mut shard_paths = Vec::new();

    println!("Creating test shards...");
    for i in 0..num_shards {
        let path = temp_dir.join(format!("bench_shard_{}.bin", i));
        create_test_shard(&path, samples_per_shard, sample_size).unwrap();
        shard_paths.push(path);
    }
    println!("Created {} shards with {} samples each ({} MB per sample)\n",
             num_shards, samples_per_shard, sample_size / 1024 / 1024);

    // ベンチマーク実行
    println!("Running benchmarks...\n");

    // 標準的なread()のベンチマーク（最初のシャードのみ）
    println!("1. Standard read() benchmark:");
    let std_time = benchmark_standard_read(&shard_paths[0], samples_per_shard, sample_size);
    let std_throughput = (samples_per_shard as f64 * sample_size as f64) / std_time.as_secs_f64() / 1024.0 / 1024.0;
    println!("   Time: {:?}", std_time);
    println!("   Throughput: {:.2} MB/s\n", std_throughput);

    // mmapのベンチマーク（最初のシャードのみ）
    println!("2. mmap benchmark:");
    let mmap_time = benchmark_mmap_read(&shard_paths[0], samples_per_shard, sample_size);
    let mmap_throughput = (samples_per_shard as f64 * sample_size as f64) / mmap_time.as_secs_f64() / 1024.0 / 1024.0;
    println!("   Time: {:?}", mmap_time);
    println!("   Throughput: {:.2} MB/s\n", mmap_throughput);

    // ゼロコピーローダーのベンチマーク（全シャード）
    println!("3. Zero-copy loader benchmark:");
    let zc_time = benchmark_zero_copy_loader(&shard_paths, total_samples);
    let zc_throughput = (total_samples as f64 * sample_size as f64) / zc_time.as_secs_f64() / 1024.0 / 1024.0;
    println!("   Time: {:?}", zc_time);
    println!("   Throughput: {:.2} MB/s\n", zc_throughput);

    // 比較
    println!("Comparison:");
    println!("   Standard read:  {:.2} MB/s (baseline)", std_throughput);
    println!("   mmap:          {:.2} MB/s ({:.2}x)", mmap_throughput, mmap_throughput / std_throughput);
    println!("   Zero-copy:     {:.2} MB/s ({:.2}x)", zc_throughput, zc_throughput / std_throughput);

    // クリーンアップ
    for path in &shard_paths {
        let _ = std::fs::remove_file(path);
    }
}