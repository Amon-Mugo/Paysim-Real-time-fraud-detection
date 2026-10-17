variable "github_oidc_sub_prefix" {
  description = "OIDC sub-claim prefix of the repository allowed to assume the CI role"
  type        = string
  default     = "repo:Amon-Mugo@205969589/Paysim-Real-time-fraud-detection@1388086945"
}

data "aws_iam_openid_connect_provider" "github_actions" {
  url = "https://token.actions.githubusercontent.com"
}

data "aws_dynamodb_table" "terraform_locks" {
  name = var.tfstate_lock_table_name
}

data "aws_iam_policy_document" "ci_plan_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [data.aws_iam_openid_connect_provider.github_actions.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "${var.github_oidc_sub_prefix}:ref:refs/heads/main",
        "${var.github_oidc_sub_prefix}:pull_request",
      ]
    }
  }
}

resource "aws_iam_role" "ci_plan_role" {
  name                 = "paysim-fraud-pipeline-ci-plan-role"
  assume_role_policy   = data.aws_iam_policy_document.ci_plan_trust.json
  max_session_duration = 3600

  tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
    Purpose   = "ci-terraform-plan-readonly"
  }
}

data "aws_iam_policy_document" "ci_plan_permissions" {
  statement {
    sid    = "AllowStateRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::${var.tfstate_bucket_name}",
      "arn:aws:s3:::${var.tfstate_bucket_name}/*",
    ]
  }

  statement {
    sid    = "AllowStateLock"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
    ]
    resources = [data.aws_dynamodb_table.terraform_locks.arn]
  }

  statement {
    sid    = "AllowStateLockFile"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "arn:aws:s3:::${var.tfstate_bucket_name}/terraform.tfstate.tflock",
    ]
  }
}

resource "aws_iam_role_policy" "ci_plan_permissions" {
  name   = "ci-plan-state-access"
  role   = aws_iam_role.ci_plan_role.id
  policy = data.aws_iam_policy_document.ci_plan_permissions.json
}

resource "aws_iam_role_policy_attachment" "ci_plan_readonly" {
  role       = aws_iam_role.ci_plan_role.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

output "ci_plan_role_arn" {
  description = "ARN to set as the AWS_CI_ROLE_ARN GitHub Actions secret"
  value       = aws_iam_role.ci_plan_role.arn
}