# Karpenter Terraform Module
#
# Implements auto-scaling for EKS nodes with scale-to-zero capability
# Significantly reduces costs by terminating nodes when no workloads are running

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "~> 1.14"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

locals {
  karpenter_namespace = "karpenter"

  common_tags = merge(
    var.common_tags,
    {
      Component = "Karpenter"
      ManagedBy = "Terraform"
    }
  )
}

# Get current AWS region for kubectl configuration
data "aws_region" "current" {}

# Karpenter Controller IAM Role
resource "aws_iam_role" "karpenter_controller" {
  name = "${var.project_name}-karpenter-controller-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = var.oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${replace(var.oidc_provider_arn, "/^(.*provider/)/", "")}:aud" = "sts.amazonaws.com"
            "${replace(var.oidc_provider_arn, "/^(.*provider/)/", "")}:sub" = "system:serviceaccount:${local.karpenter_namespace}:karpenter"
          }
        }
      }
    ]
  })

  tags = local.common_tags
}

# Karpenter Controller Policy
resource "aws_iam_role_policy" "karpenter_controller" {
  name = "karpenter-controller-policy"
  role = aws_iam_role.karpenter_controller.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateFleet",
          "ec2:CreateLaunchTemplate",
          "ec2:CreateTags",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeImages",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceTypeOfferings",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplates",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSpotPriceHistory",
          "ec2:DescribeSubnets",
          "ec2:DeleteLaunchTemplate",
          "ec2:RunInstances",
          "ec2:TerminateInstances"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = var.node_instance_role_arn
      },
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster"
        ]
        Resource = var.cluster_arn
      },
      {
        Effect = "Allow"
        Action = [
          "iam:GetInstanceProfile",
          "iam:CreateInstanceProfile",
          "iam:DeleteInstanceProfile",
          "iam:AddRoleToInstanceProfile",
          "iam:RemoveRoleFromInstanceProfile"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "pricing:GetProducts"
        ]
        Resource = "*"
      }
    ]
  })
}

# Install Karpenter using Helm
resource "helm_release" "karpenter" {
  name             = "karpenter"
  repository       = "oci://public.ecr.aws/karpenter"
  chart            = "karpenter"
  version          = "1.0.1"
  namespace        = local.karpenter_namespace
  create_namespace = true

  set {
    name  = "settings.clusterName"
    value = var.cluster_name
  }

  set {
    name  = "settings.clusterEndpoint"
    value = var.cluster_endpoint
  }

  set {
    name  = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = aws_iam_role.karpenter_controller.arn
  }

  set {
    name  = "controller.resources.requests.cpu"
    value = "100m"
  }

  set {
    name  = "controller.resources.requests.memory"
    value = "256Mi"
  }

  set {
    name  = "controller.resources.limits.cpu"
    value = "1"
  }

  set {
    name  = "controller.resources.limits.memory"
    value = "1Gi"
  }

  depends_on = [
    aws_iam_role_policy.karpenter_controller
  ]
}

# Wait for Karpenter deployment to be ready before creating manifests
# This ensures the webhook service is available to validate EC2NodeClass and NodePool resources
resource "null_resource" "wait_for_karpenter" {
  # Trigger re-execution whenever Helm release changes
  triggers = {
    helm_revision = helm_release.karpenter.metadata[0].revision
    always_run    = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      aws eks update-kubeconfig --name ${var.cluster_name} --region ${data.aws_region.current.name} --kubeconfig /tmp/kubeconfig-karpenter
      export KUBECONFIG=/tmp/kubeconfig-karpenter
      
      echo "Waiting for Karpenter deployment to be ready..."
      kubectl wait --for=condition=available --timeout=180s \
        deployment/karpenter -n ${local.karpenter_namespace}
      
      echo "Waiting for Karpenter pods to be running..."
      kubectl wait --for=condition=ready --timeout=60s \
        pod -l app.kubernetes.io/name=karpenter -n ${local.karpenter_namespace}
      
      echo "Patching CRD webhook configurations to use correct namespace..."
      kubectl patch crd ec2nodeclasses.karpenter.k8s.aws --type='json' \
        -p='[{"op": "replace", "path": "/spec/conversion/webhook/clientConfig/service/namespace", "value": "'${local.karpenter_namespace}'"}]'
      
      kubectl patch crd nodeclaims.karpenter.sh --type='json' \
        -p='[{"op": "replace", "path": "/spec/conversion/webhook/clientConfig/service/namespace", "value": "'${local.karpenter_namespace}'"}]'
      
      kubectl patch crd nodepools.karpenter.sh --type='json' \
        -p='[{"op": "replace", "path": "/spec/conversion/webhook/clientConfig/service/namespace", "value": "'${local.karpenter_namespace}'"}]'
      
      echo "Karpenter webhook namespace patched successfully!"
      rm -f /tmp/kubeconfig-karpenter
    EOT

    interpreter = ["/bin/bash", "-c"]
  }

  depends_on = [
    helm_release.karpenter
  ]
}

# EC2NodeClass for Karpenter nodes
resource "kubectl_manifest" "ec2_node_class" {
  yaml_body = yamlencode({
    apiVersion = "karpenter.k8s.aws/v1beta1"
    kind       = "EC2NodeClass"
    metadata = {
      name = "${var.project_name}-${var.environment}"
    }
    spec = {
      amiFamily = "AL2"
      role      = var.node_instance_role_name
      subnetSelectorTerms = [
        {
          tags = {
            "karpenter.sh/discovery" = var.cluster_name
          }
        }
      ]
      securityGroupSelectorTerms = [
        {
          tags = {
            "karpenter.sh/discovery" = var.cluster_name
          }
        }
      ]
      tags = merge(
        local.common_tags,
        {
          Name                     = "${var.project_name}-karpenter-node-${var.environment}"
          "karpenter.sh/discovery" = var.cluster_name
        }
      )
    }
  })

  depends_on = [
    null_resource.wait_for_karpenter
  ]
}

# NodePool for Spark workloads
resource "kubectl_manifest" "spark_node_pool" {
  yaml_body = yamlencode({
    apiVersion = "karpenter.sh/v1beta1"
    kind       = "NodePool"
    metadata = {
      name = "spark-workloads"
    }
    spec = {
      template = {
        metadata = {
          labels = {
            workload = "spark"
          }
        }
        spec = {
          nodeClassRef = {
            name = "${var.project_name}-${var.environment}"
          }
          requirements = [
            {
              key      = "karpenter.sh/capacity-type"
              operator = "In"
              values   = ["spot", "on-demand"]
            },
            {
              key      = "kubernetes.io/arch"
              operator = "In"
              values   = ["amd64"]
            },
            {
              key      = "karpenter.k8s.aws/instance-family"
              operator = "In"
              values   = ["r5", "r6i", "m5", "m6i"]
            },
            {
              key      = "karpenter.k8s.aws/instance-size"
              operator = "In"
              values   = ["xlarge", "2xlarge", "4xlarge"]
            }
          ]
          taints = []
        }
      }
      limits = {
        cpu    = "100"
        memory = "400Gi"
      }
      disruption = {
        consolidationPolicy = "WhenUnderutilized"
        expireAfter         = "720h"
      }
    }
  })

  depends_on = [
    kubectl_manifest.ec2_node_class
  ]
}

# NodePool for system workloads (Spark Operator, History Server)
resource "kubectl_manifest" "system_node_pool" {
  yaml_body = yamlencode({
    apiVersion = "karpenter.sh/v1beta1"
    kind       = "NodePool"
    metadata = {
      name = "system-workloads"
    }
    spec = {
      template = {
        metadata = {
          labels = {
            workload = "system"
          }
        }
        spec = {
          nodeClassRef = {
            name = "${var.project_name}-${var.environment}"
          }
          requirements = [
            {
              key      = "karpenter.sh/capacity-type"
              operator = "In"
              values   = ["on-demand"]
            },
            {
              key      = "kubernetes.io/arch"
              operator = "In"
              values   = ["amd64"]
            },
            {
              key      = "karpenter.k8s.aws/instance-family"
              operator = "In"
              values   = ["t3", "t3a"]
            },
            {
              key      = "karpenter.k8s.aws/instance-size"
              operator = "In"
              values   = ["medium", "large"]
            }
          ]
          taints = []
        }
      }
      limits = {
        cpu    = "4"
        memory = "16Gi"
      }
      disruption = {
        consolidationPolicy = "WhenEmpty"
        consolidateAfter    = "30s"
      }
    }
  })

  depends_on = [
    kubectl_manifest.ec2_node_class
  ]
}
