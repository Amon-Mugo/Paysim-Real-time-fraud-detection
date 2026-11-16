data "aws_iam_policy_document" "cd_trust" {
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
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${var.github_oidc_sub_prefix}:environment:production"]
    }
  }
}

resource "aws_iam_role" "cd_role" {
  name                 = "paysim_fraud_pipeline_cd_deploy_role"
  assume_role_policy   = data.aws_iam_policy_document.cd_trust.json
  max_session_duration = 3600

  tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
    Purpose   = "cd-image-deploy"
  }
}

data "aws_iam_policy_document" "cd_permissions" {
  statement {
    sid    = "EcrPushImages"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
      "ecr:PutImage",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
      "ecr:DescribeImages",
    ]
    resources = [
      "arn:aws:ecr:${var.aws_region}:${data.aws_caller_identity.current.account_id}:repository/${var.ecr_repository_name}",
    ]
  }

  statement {
    sid       = "EcrAuthToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "EmrServerlessUpdateApplication"
    effect = "Allow"
    actions = [
      "emr-serverless:GetApplication",
      "emr-serverless:UpdateApplication",
      "emr-serverless:StartApplication",
      "emr-serverless:StopApplication",
    ]
    resources = [
      "arn:aws:emr-serverless:${var.aws_region}:${data.aws_caller_identity.current.account_id}:/applications/*",
    ]
  }

  statement {
    sid       = "EmrServerlessListApplications"
    effect    = "Allow"
    actions   = ["emr-serverless:ListApplications"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "cd_permissions" {
  name   = "cd-deploy-permissions"
  role   = aws_iam_role.cd_role.id
  policy = data.aws_iam_policy_document.cd_permissions.json
}

output "cd_role_arn" {
  description = "ARN to set as the CD_ROLE_ARN GitHub Actions repository variable"
  value       = aws_iam_role.cd_role.arn
}
