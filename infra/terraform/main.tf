provider "aws" {
  region = "sa-east-1"
}

resource "aws_s3_bucket" "rosie_bucket" {
  bucket = "rosie-deployment-bucket"
  acl    = "private"
}

resource "aws_s3_bucket_object" "rosie_files" {
  for_each = fileset("${path.module}/files", "*")
  bucket   = aws_s3_bucket.rosie_bucket.bucket
  key      = each.value
  source   = "${path.module}/files/${each.value}"
}

resource "aws_glue_job" "rosie_glue_job_1" {
  name     = "rosie-glue-job-1"
  role_arn = "arn:aws:iam::123456789012:role/rosie-glue-role" # Substitua pelo ARN da role existente

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.rosie_bucket.bucket}/script1.py"
    python_version  = "3"
  }
}

resource "aws_glue_job" "rosie_glue_job_2" {
  name     = "rosie-glue-job-2"
  role_arn = "arn:aws:iam::123456789012:role/rosie-glue-role" # Substitua pelo ARN da role existente

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.rosie_bucket.bucket}/script2.py"
    python_version  = "3"
  }
}

resource "aws_glue_job" "rosie_glue_job_3" {
  name     = "rosie-glue-job-3"
  role_arn = "arn:aws:iam::123456789012:role/rosie-glue-role" # Substitua pelo ARN da role existente

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.rosie_bucket.bucket}/script3.py"
    python_version  = "3"
  }
}

resource "aws_glue_job" "rosie_glue_job_4" {
  name     = "rosie-glue-job-4"
  role_arn = "arn:aws:iam::123456789012:role/rosie-glue-role" # Substitua pelo ARN da role existente

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.rosie_bucket.bucket}/script4.py"
    python_version  = "3"
  }
}

resource "aws_cloudwatch_event_rule" "rosie_event_rule" {
  name                = "rosie-event-rule"
  schedule_expression = "rate(1 day)"
}

resource "aws_cloudwatch_event_target" "rosie_event_target" {
  rule      = aws_cloudwatch_event_rule.rosie_event_rule.name
  arn       = aws_stepfunctions_state_machine.rosie_state_machine.arn
  role_arn  = "arn:aws:iam::123456789012:role/rosie-event-bridge-role" # Substitua pelo ARN da role existente
}

resource "aws_stepfunctions_state_machine" "rosie_state_machine" {
  name     = "rosie-state-machine"
  role_arn = "arn:aws:iam::123456789012:role/rosie-step-functions-role" # Substitua pelo ARN da role existente

  definition = jsonencode({
    Comment = "State machine to orchestrate AWS Glue jobs"
    StartAt = "GlueJob1"
    States = {
      GlueJob1 = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.rosie_glue_job_1.name
        }
        Next = "GlueJob2"
      }
      GlueJob2 = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.rosie_glue_job_2.name
        }
        Next = "GlueJob3"
      }
      GlueJob3 = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.rosie_glue_job_3.name
        }
        Next = "GlueJob4"
      }
      GlueJob4 = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.rosie_glue_job_4.name
        }
        End = true
      }
    }
  })
}