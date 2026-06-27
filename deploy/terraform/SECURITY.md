# Weft EKS security controls — threat model & consumption guide

> This document covers the **security-hardening** Terraform in this directory
> (`versions.tf`, `variables.tf`, `main.tf`, `outputs.tf`, and
> `modules/cluster-irsa/`). It complements the broader-infra overview in
> `README.md` (VPC, RDS, ALB, ECR), which is owned by a separate stack — this
> stack consumes that infra as inputs (`subnet_ids`, `workspace_bucket`) and
> stays a focused, independently-reviewable security boundary.

## Architecture in one paragraph

A control-plane **gateway** provisions per-user compute "clusters" as hardened
pods. Each pod reads **AWS Glue** (table metadata) and **S3** (data) using
**IRSA** — the pod's projected ServiceAccount token is exchanged at STS for
credentials of a per-cluster IAM role. The trust boundary that makes this safe
is that each role can be assumed by **exactly one** pod identity and grants
**exactly one** S3 prefix + **one** Glue database. The controls below exist to
make sure that boundary cannot be bypassed (by token confusion, by stealing the
node role through IMDS, or by reading Secrets out of etcd).

---

## Control #1 — IRSA roles pinned on BOTH `sub` and `aud`

**Where:** `modules/cluster-irsa/main.tf` (`data.aws_iam_policy_document.trust`).

**Threat.** Every pod in the cluster receives a projected SA token signed by the
*same* OIDC issuer. If a role's trust policy federates against only the OIDC
**provider ARN**, then *any* pod that can reach STS could assume *any* such role
— a complete failure of per-tenant isolation. An attacker who lands in tenant
A's pod could assume tenant B's role and read B's data.

**Mitigation.** The trust policy adds two `StringEquals` conditions on the token
claims, which are the real security boundary:

```hcl
condition {                                   # pin the EXACT ServiceAccount
  test     = "StringEquals"
  variable = "<issuer>:sub"
  values   = ["system:serviceaccount:<namespace>:<service_account>"]
}
condition {                                   # pin the audience
  test     = "StringEquals"
  variable = "<issuer>:aud"
  values   = ["sts.amazonaws.com"]
}
```

- **`:sub`** ties the role to one `system:serviceaccount:<ns>:<sa>`. Only the
  pod running as that ServiceAccount can assume it.
- **`:aud`** rejects tokens minted for any other audience. Kubernetes can
  project tokens for arbitrary audiences (e.g. an in-cluster service); pinning
  `aud=sts.amazonaws.com` stops such a token from being replayed against
  `sts:AssumeRoleWithWebIdentity`.

**Least privilege (no wildcards).** The inline policy grants only:
- `s3:ListBucket`/`GetBucketLocation` on the one bucket, **conditioned** on
  `s3:prefix` so only the tenant's prefix is enumerable;
- `s3:GetObject` on `…/<prefix>/*` only;
- `glue:GetDatabase`/`GetTable(s)`/`GetPartition(s)` on the **one** database, its
  tables, and the account catalog ARN only;
- optionally `kms:Decrypt`/`DescribeKey` on **one** CMK, and only if the data is
  SSE-KMS.

There are no `*` actions and no `*` resources for the data grants.

---

## Control #2 — Gateway IRSA role is minimal and pinned

**Where:** `main.tf` (`aws_iam_role.gateway`, `data.aws_iam_policy_document.gateway_trust`).

**Threat.** The gateway is internet-adjacent (it terminates user requests). If it
held broad AWS rights — e.g. `iam:CreateRole`, `iam:PassRole`, `sts:AssumeRole`
on the per-cluster roles — a gateway compromise would be a privilege-escalation
springboard into every tenant's data.

**Mitigation.** The gateway provisions compute pods **without** any AWS IAM call:
the per-cluster roles are pre-provisioned by Terraform (control #1) and the
gateway merely **annotates** each pod's ServiceAccount with the correct role ARN
— a pure Kubernetes API operation. Therefore the gateway role needs **no standing
AWS permissions** and by default has an empty inline policy. Its trust policy is
still `sub`+`aud` pinned (same pattern as #1) so it can't be assumed by another
pod. The only optional grant is a single read-only `ec2:DescribeInstances` for a
legacy node-topology lookup (`var.gateway_allow_ec2_describe`, default `false`).
`ec2:Describe*` cannot be resource-scoped, but it is read-only and exposes no
data-plane content; it is created only when the flag is on.

---

## Control #3 — IMDSv2 required, metadata hop limit = 1

**Where:** `main.tf` (`aws_launch_template.node` → `metadata_options`).

**Threat.** A compromised pod curls the node's Instance Metadata Service at
`169.254.169.254` and reads the **node role** credentials. The node role
(`AmazonEKSWorkerNodePolicy`, CNI, ECR pull) is much broader than any per-cluster
IRSA role and is shared by every pod on the node — reading it is a full
**IRSA-bypass**: it sidesteps the careful `sub`/`aud`/prefix scoping of #1.

**Mitigation (two layers):**

```hcl
metadata_options {
  http_endpoint               = "enabled"
  http_tokens                 = "required" # IMDSv2 only — no unauthenticated IMDSv1
  http_put_response_hop_limit = 1          # node-only; pod is a 2nd hop -> TTL expires
}
```

- `http_tokens = "required"` disables IMDSv1, so a plain unauthenticated `GET`
  no longer returns credentials; a caller must first `PUT` for a session token.
- `http_put_response_hop_limit = 1` sets the IP TTL on the IMDS response to 1.
  The node is hop 1; a container in a pod sits behind the node's network stack
  and is hop 2, so the token-handshake reply's TTL hits 0 before it reaches the
  pod. Pods therefore **cannot complete the IMDSv2 handshake at all**.

Legitimate pods never need IMDS — they get AWS credentials via IRSA (projected
token → STS) — so clamping IMDS to the node breaks nothing. If a Karpenter
`EC2NodeClass` (or any non-managed-node-group provisioner) owns capacity instead,
it **must** set the same `metadataOptions` (`httpTokens: required`,
`httpPutResponseHopLimit: 1`); the launch template id is exported for reuse.

---

## Control #4 — Kubernetes Secrets encrypted at rest in etcd

**Where:** `main.tf` (`aws_kms_key.eks_secrets` + `aws_eks_cluster.this.encryption_config`).

**Threat.** Without envelope encryption, Kubernetes Secrets live in etcd as
base64 (effectively plaintext). Anyone with an etcd snapshot/backup, or etcd read
access, recovers every Secret: the RDS metastore password, the Anthropic API key,
TLS keys, etc.

**Mitigation.** A customer-managed KMS key (rotation enabled) envelope-encrypts
the `secrets` resource:

```hcl
encryption_config {
  provider { key_arn = aws_kms_key.eks_secrets.arn }
  resources = ["secrets"]
}
```

The key policy grants the account root full administration (no lockout) and
grants the EKS control-plane role only the envelope-encryption verbs
(`Encrypt`/`Decrypt`/`GenerateDataKey*`/`DescribeKey`/`CreateGrant`) — not key
administration. Secrets are now ciphertext at rest; decryption requires the CMK.

---

## Control #5 — EKS OIDC provider (the IRSA trust anchor)

**Where:** `main.tf` (`aws_iam_openid_connect_provider.eks`, fed by
`aws_eks_cluster.this.identity[0].oidc[0].issuer`).

This is the federation root every IRSA role in #1 and #2 depends on. The issuer
URL is taken from the cluster, its TLS thumbprint is computed via the `tls`
provider, and the STS audience is allow-listed. Controls #1/#2 then *pin* `sub`
and `aud` on top of it. Without this resource there is no web-identity
federation and `module.cluster_irsa` has nothing to trust.

---

## How the gateway / Helm consume the outputs

`terraform output` exposes the role ARNs the platform wires into Kubernetes:

| Output | Consumer | Use |
| --- | --- | --- |
| `oidc_provider_arn`, `cluster_oidc_issuer_url` | platform/IaC | trust anchor for any out-of-band IRSA roles |
| `gateway_role_arn` | Helm values | annotate the gateway ServiceAccount: `eks.amazonaws.com/role-arn: <arn>` |
| `cluster_irsa_role_arns` | gateway / operator | map `cluster-id → role ARN`; stamped onto each compute pod's ServiceAccount |
| `cluster_irsa_sa_annotations` | gateway / operator | same, pre-shaped as the `{eks.amazonaws.com/role-arn = …}` annotation map |
| `secrets_kms_key_arn` | audit | proves etcd Secret encryption is active |
| `node_launch_template_id` | node groups / Karpenter | reuse, or mirror its `metadata_options` |

**The IRSA contract (must match exactly):** a role's trust `sub` is
`system:serviceaccount:<namespace>:<service_account>`. The pod that assumes it
**must** run in that `<namespace>` under a ServiceAccount of that
`<service_account>` name annotated with the role ARN. The gateway (or the
`WeftCluster` operator) is responsible for creating each compute pod with the
namespace/SA that the role in `var.clusters` was pinned to.

### Pre-provisioning per-cluster roles

```hcl
module "weft_security" {            # or: terraform apply in this directory
  source           = "./deploy/terraform"
  cluster_name     = "weft-prod"
  subnet_ids       = var.private_subnet_ids
  workspace_bucket = "weft-prod-workspace"

  clusters = {
    "tenant-acme-analytics" = {
      namespace       = "weft-clusters"
      service_account = "acme-analytics"
      s3_prefix       = "tenants/acme/analytics"   # reads s3://.../tenants/acme/analytics/*
      glue_database   = "acme_analytics"           # reads only this Glue db
      s3_kms_key_arn  = ""                          # set if the data bucket is SSE-KMS
    }
  }
}
```

The resulting role ARN appears in `cluster_irsa_role_arns["tenant-acme-analytics"]`
and the operator annotates the `acme-analytics` ServiceAccount in the
`weft-clusters` namespace with it. That pod — and only that pod — can read that
one prefix and that one database, with no path to the node role (control #3) or
to etcd Secrets in plaintext (control #4).
