# Outbound internet path for the private subnets (EMR workers -> Snowflake).
# A single NAT gateway keeps cost low; destroy it with MSK after each session.

resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.paysim_fraud_pipeline.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 100)
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name        = "paysim-fraud-pipeline-public-0"
    Environment = var.environment
  }
}

resource "aws_internet_gateway" "paysim_fraud_pipeline" {
  vpc_id = aws_vpc.paysim_fraud_pipeline.id

  tags = {
    Name        = "paysim-fraud-pipeline-igw"
    Environment = var.environment
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.paysim_fraud_pipeline.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.paysim_fraud_pipeline.id
  }

  tags = {
    Name        = "paysim-fraud-pipeline-public-rt"
    Environment = var.environment
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name        = "paysim-fraud-pipeline-nat-eip"
    Environment = var.environment
  }
}

resource "aws_nat_gateway" "paysim_fraud_pipeline" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id

  tags = {
    Name        = "paysim-fraud-pipeline-nat"
    Environment = var.environment
  }

  depends_on = [aws_internet_gateway.paysim_fraud_pipeline]
}

resource "aws_route" "private_default" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.paysim_fraud_pipeline.id
}