###############################################################################
# modules/cluster-irsa — per-cluster scoped IRSA role
###############################################################################

locals {
  role_name = var.role_name != "" ? var.role_name : "${var.cluster_name}-irsa-${var.cluster_id}"

  # ARN components for the ONE Glue database this role may read.
  glue_catalog_arn  = "arn:${var.partition}:glue:${var.aws_region}:${var.aws_account_id}:catalog"
  glue_database_arn = "arn:${var.partition}:glue:${var.aws_region}:${var.aws_account_id}:database/${var.glue_database_name}"
  glue_tables_arn   = "arn:${var.partition}:glue:${var.aws_region}:${var.aws_account_id}:table/${var.glue_database_name}/*"
}

###############################################################################
# Trust policy — control #1: pin BOTH sub AND aud.
#
# Federating against the OIDC provider ARN alone is NOT enough: every pod in the
# cluster gets a projected token signed by the same issuer, so a provider-only
# trust would let ANY pod assume THIS role. The two StringEquals conditions are
# the actual security boundary:
#
#   <issuer>:sub == system:serviceaccount:<ns>:<sa>
#       -> only the one ServiceAccount backing this cluster's pod can assume it.
#   <issuer>:aud == sts.amazonaws.com
#       -> only tokens minted for STS are accepted; a projected token issued for
#          some other audience (e.g. an in-cluster service) can't be replayed
#          against AssumeRoleWithWebIdentity.
###############################################################################

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "EksIrsaSubAudPinned"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    # Pin the exact ServiceAccount (sub).
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.namespace}:${var.service_account}"]
    }

    # Pin the audience (aud).
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:aud"
      values   = [var.audience]
    }
  }
}

###############################################################################
# Inline permissions — least privilege, NO wildcards on identity.
#   * S3: list within and read objects under exactly ONE prefix.
#   * Glue: read metadata of exactly ONE database (+ its tables, + the catalog).
#   * KMS: decrypt only, only the data key, only if the bucket is SSE-KMS.
###############################################################################

data "aws_iam_policy_document" "permissions" {
  # --- S3 list: only keys under the one prefix are enumerable ---
  statement {
    sid       = "ListOneS3Prefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:${var.partition}:s3:::${var.s3_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.s3_prefix}/*", var.s3_prefix]
    }
  }

  # --- S3 read: only objects under the one prefix ---
  statement {
    sid       = "ReadOneS3Prefix"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["arn:${var.partition}:s3:::${var.s3_bucket}/${var.s3_prefix}/*"]
  }

  # --- Glue read: metadata of exactly one database ---
  statement {
    sid    = "ReadOneGlueDatabase"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      local.glue_catalog_arn,  # required parent resource for Glue reads
      local.glue_database_arn, # the one database
      local.glue_tables_arn,   # its tables only
    ]
  }

  # --- KMS decrypt: only when the data is SSE-KMS, only that key, decrypt only ---
  dynamic "statement" {
    for_each = var.s3_kms_key_arn != "" ? [var.s3_kms_key_arn] : []
    content {
      sid       = "DecryptS3DataKey"
      effect    = "Allow"
      actions   = ["kms:Decrypt", "kms:DescribeKey"]
      resources = [statement.value]
    }
  }
}

resource "aws_iam_role" "this" {
  name                 = local.role_name
  description          = "Scoped IRSA role for Weft compute cluster '${var.cluster_id}' (sub+aud pinned; reads s3://${var.s3_bucket}/${var.s3_prefix} and Glue db ${var.glue_database_name})."
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  max_session_duration = var.max_session_duration

  tags = merge(var.tags, {
    Name           = local.role_name
    "weft:cluster" = var.cluster_id
  })
}

resource "aws_iam_role_policy" "this" {
  name   = "scoped-s3-glue-read"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.permissions.json
}
