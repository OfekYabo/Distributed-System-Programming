#!/bin/bash
set -e

# Check for .env file
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please create a .env file with S3_BUCKET_NAME and AWS_REGION."
    exit 1
fi

echo "Loading configuration from .env..."
# Grep the values from .env, handling potential Windows line endings
S3_BUCKET=$(grep "^S3_BUCKET_NAME=" .env | cut -d '=' -f2 | tr -d '\r')
AWS_REGION=$(grep "^AWS_REGION=" .env | cut -d '=' -f2 | tr -d '\r')

if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET_NAME not defined in .env"
    exit 1
fi

if [ -z "$AWS_REGION" ]; then
    echo "Error: AWS_REGION not defined in .env"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
DEPLOY_MANAGER=false
DEPLOY_WORKER=false

if [ $# -eq 0 ]; then
    # No arguments - deploy both
    DEPLOY_MANAGER=true
    DEPLOY_WORKER=true
else
    for arg in "$@"; do
        case $arg in
            manager)
                DEPLOY_MANAGER=true
                ;;
            worker)
                DEPLOY_WORKER=true
                ;;
            all)
                DEPLOY_MANAGER=true
                DEPLOY_WORKER=true
                ;;
            *)
                echo "Usage: $0 [manager|worker|all]"
                echo "  manager  - Build and deploy manager.jar only"
                echo "  worker   - Build and deploy worker.jar only"
                echo "  all      - Build and deploy both (default if no args)"
                exit 1
                ;;
        esac
    done
fi

echo "================================================"
echo "  Distributed Systems - Deploy Script"
echo "================================================"
echo ""
echo "Region: $AWS_REGION"
echo "Bucket: $S3_BUCKET"
echo ""

# Check AWS CLI is installed and configured
echo "Checking AWS credentials..."
if ! aws sts get-caller-identity &>/dev/null; then
    print_error "AWS credentials not configured or expired"
    echo ""
    echo "Please configure AWS credentials in ~/.aws/credentials:"
    echo ""
    echo "[default]"
    echo "aws_access_key_id = <your-access-key>"
    echo "aws_secret_access_key = <your-secret-key>"
    echo "aws_session_token = <your-session-token>"
    echo ""
    echo "Or run: aws configure"
    exit 1
fi
IDENTITY=$(aws sts get-caller-identity --query 'Arn' --output text 2>/dev/null)
print_status "AWS credentials valid ($IDENTITY)"

# Create S3 bucket if it doesn't exist
echo ""
echo "Checking S3 bucket..."
if ! aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
    echo "Creating bucket $S3_BUCKET..."
    aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION"
    print_status "Bucket created: $S3_BUCKET"
else
    print_status "Bucket exists: $S3_BUCKET"
fi

# Build and deploy manager
if [ "$DEPLOY_MANAGER" = true ]; then
    echo ""
    echo "================================================"
    echo "  Building Manager"
    echo "================================================"
    cd "$SCRIPT_DIR/manager"
    
    echo "Running Maven build..."
    mvn clean package -DskipTests -q
    
    if [ -f "target/manager-1.0-SNAPSHOT.jar" ]; then
        print_status "Manager built successfully"
        
        echo "Uploading to S3..."
        aws s3 cp target/manager-1.0-SNAPSHOT.jar "s3://$S3_BUCKET/manager.jar"
        print_status "Uploaded manager.jar to s3://$S3_BUCKET/manager.jar"
    else
        print_error "Manager JAR not found!"
        exit 1
    fi
fi

# Build and deploy worker
if [ "$DEPLOY_WORKER" = true ]; then
    echo ""
    echo "================================================"
    echo "  Building Worker"
    echo "================================================"
    cd "$SCRIPT_DIR/worker"
    
    echo "Running Maven build..."
    mvn clean package -DskipTests -q
    
    if [ -f "target/worker-1.0-SNAPSHOT.jar" ]; then
        print_status "Worker built successfully"
        
        echo "Uploading to S3..."
        aws s3 cp target/worker-1.0-SNAPSHOT.jar "s3://$S3_BUCKET/worker.jar"
        print_status "Uploaded worker.jar to s3://$S3_BUCKET/worker.jar"
    else
        print_error "Worker JAR not found!"
        exit 1
    fi
fi

echo ""
echo "================================================"
echo -e "  ${GREEN}Deployment Complete!${NC}"
echo "================================================"
echo ""
echo "Deployed files:"
if [ "$DEPLOY_MANAGER" = true ]; then
    echo "  - s3://$S3_BUCKET/manager.jar"
fi
if [ "$DEPLOY_WORKER" = true ]; then
    echo "  - s3://$S3_BUCKET/worker.jar"
fi
echo ""

