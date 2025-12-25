# Task Checklist - Shared AWS Services Refactoring

- [ ] **Research & Design**
    - [x] Analyze existing AWS usage in LocalApp, Manager, and Worker
    - [x] Identify similarities and differences
    - [x] Design shared service layer (Implementation Plan: `shared_services_plan.md`)

- [ ] **Project Structure Setup**
    - [ ] Create root `pom.xml`
    - [ ] Create `shared-lib` module structure
    - [ ] Update `local-app`, `manager`, `worker` to be modules of root

- [ ] **Implement Shared Library**
    - [ ] Create `AwsClientFactory`
    - [ ] Implement `S3Service` (generalized)
    - [ ] Implement `SqsService` (generalized)
    - [ ] Implement `Ec2Service` (generalized)
    - [ ] Move shared models (if any)

- [ ] **Refactor LocalApp**
    - [ ] Update `pom.xml` to depend on `shared-lib`
    - [ ] Replace local services with shared services
    - [ ] Verify LocalApp build

- [ ] **Refactor Manager**
    - [ ] Update `pom.xml` to depend on `shared-lib`
    - [ ] Replace local services with shared services
    - [ ] Verify Manager build

- [ ] **Refactor Worker**
    - [ ] Update `pom.xml` to depend on `shared-lib`
    - [ ] Use `AwsClientFactory` and shared services
    - [ ] Verify Worker build

- [ ] **Verification**
    - [ ] Run `mvn clean install` on root
    - [ ] Perform End-to-End test (if environment allows)
