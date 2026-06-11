from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def main() -> int:
	repo_root = os.path.dirname(os.path.abspath(__file__))
	packages = ",".join(
		[
			"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
			"org.apache.hadoop:hadoop-aws:3.3.4",
			"com.amazonaws:aws-java-sdk-bundle:1.12.262",
			"org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
			"org.apache.commons:commons-pool2:2.11.1",
		]
	)

	env_args: list[str] = []
	for key in [
		"MINIO_ENDPOINT",
		"MINIO_ACCESS_KEY",
		"MINIO_SECRET_KEY",
		"MINIO_BUCKET",
		"EDGE_DEVICE_ICEBERG_NAMESPACE",
	]:
		if key in os.environ:
			env_args.extend(["-e", f"{key}={os.environ[key]}"])

	container_output_dir = os.getenv("EDGE_DEVICE_CSV_OUTPUT_DIR", "/tmp/edge_device_csv")
	env_args.extend(["-e", f"EDGE_DEVICE_CSV_OUTPUT_DIR={container_output_dir}"])

	cmd = [
		"docker",
		"compose",
		"exec",
		"-T",
		*env_args,
		"spark-master",
		"spark-submit",
		"--conf",
		"spark.jars.ivy=/tmp/.ivy2",
		"--packages",
		packages,
		"/opt/spark_jobs/edge_device_reader_job.py",
	]

	env = os.environ.copy()
	result = subprocess.run(cmd, cwd=repo_root, env=env)
	if result.returncode != 0:
		return result.returncode

	host_output_dir = Path(repo_root) / "output" / "edge_device"
	host_output_dir.mkdir(parents=True, exist_ok=True)

	copy_cmd = [
		"docker",
		"compose",
		"cp",
		f"spark-master:{container_output_dir}/.",
		str(host_output_dir),
	]
	copy_result = subprocess.run(copy_cmd, cwd=repo_root, env=env)
	if copy_result.returncode != 0:
		return copy_result.returncode

	print(f"CSV files copied to: {host_output_dir}")
	return result.returncode


if __name__ == "__main__":
	raise SystemExit(main())
