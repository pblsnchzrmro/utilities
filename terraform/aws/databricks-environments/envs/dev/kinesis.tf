resource "aws_kinesis_stream" "basic_stream" {
  name        = "kinesis-stream-pblsnchzrmr"
  shard_count = 1
}
