# CI/CD (Jenkins)

This repository uses **Jenkins** to implement Continuous Integration and Continuous Deployment (CI/CD)
for the **application layer only**.

Infrastructure components are provisioned separately and are **explicitly out of scope** for CI/CD.

---

## Scope

### What Jenkins manages

Jenkins is responsible for:

- Running unit tests with coverage enforcement
- Building application Docker images
- Pushing images to **Google Artifact Registry**
- Deploying application workloads via Helm to Kubernetes

Specifically, Jenkins deploys:

- `rag-orchestrator`
- `streamlit-ui`
- `qdrant-ingestor` (image build only; ingestion is triggered explicitly)

All deployments target the `model-serving` namespace.

---

## Pipeline Overview

The CI/CD pipeline is defined in the root-level `Jenkinsfile`.

High-level stages:

1. Source checkout
2. Unit tests (FastAPI)
3. Coverage quality gate (>= 80%)
4. Static gate (SonarQube and CheckOV)
5. Docker image build
6. Image push to Artifact Registry
7. Helm upgrade to `model-serving` namespace

## Guide to setup Jenskin
This step sets up a **self-hosted Jenkins** server with Docker support to automate image builds, pushes to Artifact Registry, and Kubernetes deployments. Jenkins runs as a Docker container with access to the host Docker daemon, enabling fully containerized CI/CD workflows.  
>Quick start: commit the current repo in your github. Also, create personal access token (classic)
### 1. **Configure Docker Permissions (Host Machine)**  
```code
sudo groupadd docker || true
sudo usermod -aG docker $USER
newgrp docker
sudo chown root:docker /var/run/docker.sock
sudo chmod 660 /var/run/docker.sock
```
### 2. **Build Custom Jenkins Image**  
```code
cd ci
docker build -t jenkins-ci:latest -f Dockerfile .
```
### 3. **Prepare Jenkins Home Directory**
```
sudo chown -R 1000:1000 ~/jenkins_home
sudo chmod -R 775 ~/jenkins_home
```
### 4. **Run Jenkins Container**  
```code
docker run -d --name jenkins \
  -p 8080:8080 \
  -p 50000:50000 \
  -v ~/jenkins_home:/var/jenkins_home \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --group-add $(getent group docker | cut -d: -f3) \
  jenkins-ci:latest
```

### 5. **Retrieve Initial Admin Password**
```code
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
OR:
docker logs jenkins | grep -A 5 password
```

### 6. **Useful Jenkins Container Commands (Reference)**
```code
docker start jenkins
docker logs jenkins
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
docker rmi -f $(docker images -a -q)

AND CTRL + D TO EXIT CONTAINER
```
From here, open link localhost: [8080](http://localhost:8080/), pass the password, you will see Jenkin UI
### 7. **Install Required Jenkins Plugins**
![](/assets/imgs/Plugins.png)

### 8. **Authenticate Jenkins with Google Cloud (One-Time)**
```code
docker exec -it jenkins bash
gcloud auth login
gcloud config set project medqa-student-project
gcloud auth configure-docker us-central1-docker.pkg.dev
```
### 9. **Exit the container, restart jenkins**
```code
docker start jenkins
```
### 10. **Open another terminal, expose Jenkins Publicly**
```code
ngrok http 8080
```  

![](/assets/imgs/ngrok.png)  

### 11. **Back to previous terminal, create GCP Service Account for Jenkins**
```code
gcloud iam service-accounts create jenkins-deployer \
  --display-name "Jenkins GKE Deployer"
```

### 12. **Grant Required IAM Roles**
```code
gcloud projects add-iam-policy-binding medqa-student-project \
  --member="serviceAccount:jenkins-deployer@medqa-student-project.iam.gserviceaccount.com" \
  --role="roles/container.developer"

gcloud projects add-iam-policy-binding medqa-student-project \
  --member="serviceAccount:jenkins-deployer@medqa-student-project.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
```

### 13. **Generate Service Account Key (do not commit and push this file)**
```code
gcloud iam service-accounts keys create jenkins-gke.json \
  --iam-account jenkins-deployer@medqa-student-project.iam.gserviceaccount.com
```
### 14. **In Jenkins UI store credentials in Jenkins**
![](/assets/imgs/Store_credential_in_jenkins.png)

### 15. **Link github to jenkin**
Back to github, access setting -> webhook -> add new github link  
![](/assets/imgs/Webhook.png)

In the `which events would you like to trigger this webhook`, select `let me select individual events`, then select:
![](/assets/imgs/pull_push.png)

Create a job -> Select `multibranch pipeline`, Click OK
![](/assets/imgs/Create_new_job.png)

Add credentials  
![](/assets/imgs/github_credential.png)


### 16. **Setup SonarQube**
Create a SonarQube cloud account, and link to github repository (Follow this guide: [Documentations](https://docs.sonarsource.com/sonarqube-cloud/getting-started/github))  

Once done github connection, create "New Analyze"  
![](/assets/imgs/Sonar_create_new_analyze.png)  

Select repo to analyze, then click "Set Up"  
![](/assets/imgs/Sonar_select_new_repo_to_analyze.png)  

Choose CI configuration, follow instructions to setup and update Jenkinsfile (stage: 'Static Code Analysis - SonarQube')  
![](/assets/imgs/Sonar_setup_instruction.png)  

Create Quality Gate  
![](/assets/imgs/Sonar_create_quality_gate.png)  

In Jenkin UI, add a global credential  
![](/assets/imgs/Sonar_add_credential.png)  

Install Sonar plugins  
![](/assets/imgs/Sonar_installed_plugins.png)  

In Jenkin UI, configure Sonar configuration  
![](/assets/imgs/Sonar_Jenkin_system_configuration.png)  


### 17. **NeMo Guardrails**  
This step is already done while deploy model-serving namespace.

### 18. **Commit and push 1 branch, observe the jenkins pipeline**
