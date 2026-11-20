#!/usr/bin/env bash
# 目录库丢失/恢复演示（Docker Compose 双实例共卷）：
#   1) 双实例启动全新卷并跨实例发布；
#   2) 备份后删除卷上的 release.db（原件保留）：
#      两个实例 /health 报告 directory_fault，发布/目录/检查点/取件/取证全 503，
#      且卷上不会被补建空库；
#   3) 恢复原 release.db 并重启：按原检查点续发（id 连续，不是从 1 重发）。
#
# 用法：bash scripts/directory_fault_demo.sh
# 演示过程中可打开 http://localhost:8001/docs 与 http://localhost:8002/docs
# 交互复核：故障期所有数据面端点返回 503，恢复后一切正常。
set -euo pipefail

VOL="water-quality-shared-data"
A="http://localhost:8001"
B="http://localhost:8002"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

vol_exec() {  # 在共享卷上执行只与文件相关的操作（辅助容器，用完即删）
  docker run --rm -v "$VOL":/data alpine:3 sh -c "$1"
}

wait_up() {  # 等待实例可响应 /health（不区分 ok / directory_fault）
  local base="$1"
  for _ in $(seq 1 60); do
    if curl -sf "$base/health" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "实例 $base 未就绪" >&2; exit 1
}

echo "== 0. 启动双实例（全新卷）=="
docker compose up -d --build
wait_up "$A"; wait_up "$B"
curl -s "$A/health"; echo
curl -s "$B/health"; echo

echo "== 1. 跨实例发布两件（A 发第 1 件，B 发第 2 件）=="
printf 'station,ph\nST01,7.1\n' > "$WORK/day1.csv"
printf 'station,ph\nST02,7.2\n' > "$WORK/day2.csv"
curl -s -X POST "$A/releases" -F station_id=ST01 -F sample_date=2026-11-16 \
  -F order_no=ORD-1 -F file=@"$WORK/day1.csv" | python3 -m json.tool | head -8
curl -s -X POST "$B/releases" -F station_id=ST02 -F sample_date=2026-11-16 \
  -F order_no=ORD-2 -F file=@"$WORK/day2.csv" | python3 -m json.tool | head -8
echo "-- 最新检查点（应为 seq=2）--"
curl -s "$A/checkpoints/latest"; echo

echo "== 2. 备份原目录库，然后模拟丢失（原件全部保留在卷上）=="
docker compose stop            # 优雅停机，WAL 收拢进主库文件
vol_exec "cp /data/release.db /data/release.db.backup"
vol_exec "rm /data/release.db"
vol_exec "ls -la /data; echo; ls -R /data/blobs | head -20"
docker compose start
wait_up "$A"; wait_up "$B"

echo "== 3. 故障报告：两个实例 /health 均报 directory_fault =="
curl -s "$A/health" | python3 -m json.tool
curl -s "$B/health" | python3 -m json.tool

echo "== 4. 数据面一律 503（发布/目录/检查点/取件/取证）=="
for path in \
  "/releases" \
  "/releases/ST01/2026-11-16/ORD-1" \
  "/releases/ST01/2026-11-16/ORD-1/download" \
  "/releases/ST01/2026-11-16/ORD-1/inclusion-proof" \
  "/checkpoints" \
  "/checkpoints/latest" \
  "/checkpoints/1" \
  "/checkpoints/1/append-proof"; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$A$path")
  echo "GET  $path -> $code"
done
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$A/releases" \
  -F station_id=ST09 -F sample_date=2026-11-16 -F order_no=ORD-9 \
  -F file=@"$WORK/day1.csv")
echo "POST /releases -> $code"

echo "== 5. 确认卷上未被补建空库，原件原样保留 =="
vol_exec "test ! -e /data/release.db && echo 'release.db 未被补建'"
vol_exec "ls /data"

echo "== 6. 恢复原目录库并重启 =="
docker compose stop
vol_exec "mv /data/release.db.backup /data/release.db"
docker compose start
wait_up "$A"; wait_up "$B"
curl -s "$A/health" | python3 -m json.tool

echo "== 7. 按原检查点续发（新记录应为 id=3，而非从 1 重发）=="
printf 'station,ph\nST03,7.3\n' > "$WORK/day3.csv"
curl -s -X POST "$B/releases" -F station_id=ST03 -F sample_date=2026-11-16 \
  -F order_no=ORD-3 -F file=@"$WORK/day3.csv" | python3 -m json.tool
echo "-- 最新检查点（应为 seq=3，根与故障前同链）--"
curl -s "$A/checkpoints/latest"; echo

echo "演示完成。可在 $A/docs 与 $B/docs 交互复核上述每一步。"
