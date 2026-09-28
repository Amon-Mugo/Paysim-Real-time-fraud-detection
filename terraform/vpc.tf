# used for establishing a secure pipeline route for the data to pass
# the communication of spark and kafka

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_region" "current" {}

variable "vpc_cidr" { 
  description = "CIDR block for fraud pipeline"
  type        = string
  default     = "10.0.0.0/16"
}

resource "aws_vpc" "paysim_fraud_pipeline" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name        = "paysim-fraud-pipeline-vpc"
    Environment = var.environment
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.paysim_fraud_pipeline.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index] 

  tags = {
    Name        = "paysim-fraud-pipeline-private-${count.index}"
    Environment = var.environment
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.paysim_fraud_pipeline.id

  tags = {
    Name        = "paysim-fraud-pipeline-private-rt"
    Environment = var.environment
  }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id 
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "paysim-fraud-pipeline-vpce-"
  vpc_id      = aws_vpc.paysim_fraud_pipeline.id

  ingress {
    description = "HTTPS from within VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp" 
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1" 
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "paysim-fraud-pipeline-vpce-sg"
    Environment = var.environment
  }

  lifecycle {
    create_before_destroy = true
  }
}

# security policy for msk and emr 
resource "aws_security_group" "msk_emr" {
  name_prefix = "paysim-fraud-pipeline-msk-emr-" 
  vpc_id      = aws_vpc.paysim_fraud_pipeline.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1" # <--- Fixed typo (was protocals)
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "paysim-fraud-pipeline-msk-emr-sg"
    Environment = var.environment
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group_rule" "msk_emr_self_ingress" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk_emr.id
  source_security_group_id = aws_security_group.msk_emr.id
  description              = "Allow all traffic between MSK and EMR only"
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.paysim_fraud_pipeline.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = {
    Name        = "paysim-fraud-pipeline-s3-endpoint"
    Environment = var.environment
  }
}

resource "aws_vpc_endpoint" "interface" {
  for_each = toset(["ecr.api", "ecr.dkr", "sts", "logs"])

  vpc_id              = aws_vpc.paysim_fraud_pipeline.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.${each.value}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Name        = "paysim-fraud-pipeline-${each.value}-endpoint"
    Environment = var.environment
  }
}

output "vpc_id" {
  value = aws_vpc.paysim_fraud_pipeline.id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "msk_emr_security_group_id" {
  value = aws_security_group.msk_emr.id
}