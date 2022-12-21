# Capstone Project - Anchisa Sittiviriyachai 64199130077

## Project instruction
[Project Instruction.pdf](https://github.com/ananmind/-swu-ds525/files/10274055/Project.Instruction.pdf)

## Project presentation
[Slide Capstone Project.pdf](https://github.com/ananmind/-swu-ds525/files/10273958/Slide.Capstone.Project.pdf)

## Data Model
![data model](https://user-images.githubusercontent.com/112712486/208810901-2649eead-8679-4189-819f-2850e5af1909.jpg)

## Cloud access AWS
### 1. Get Access Key จาก AWS
```sh
$ cat ~/.aws/credentials
```
![aws credential](https://user-images.githubusercontent.com/112712486/208808834-2556caa2-d3da-4431-ac75-73452eb8f377.jpg)
- จะได้ 3 keys ดังนี้เพื่อนำไปใส่ใน code บน Github เพื่อเชื่อมต่อ AWS
> - aws_access_key_id
> - aws_secret_access_key
> - aws_session_token
### 2. นำข้อมูลขึ้นไปไว้ใน S3 โดยเปิดให้เป็น public
![S3](https://user-images.githubusercontent.com/112712486/208809459-ffe0b5d6-f6fc-428f-8ace-0137ac2bd980.jpg)
### 3. สร้าง Redshift โดยให้ Node type เป็น ra3.xplus และมีจำนวน 1node ตั้ง username และ password ให้เรียบร้อย และเปิดให้เป็น public
![redshift](https://user-images.githubusercontent.com/112712486/208809606-0dc72e79-1790-4b74-9fb8-1278aa3bb4c0.jpg)

## Github
### 1. Change directory  
```sh
$ cd 08-capstone-project
```

### 2.	สร้าง virtual environment ชื่อ "ENV" (ในครั้งแรก)
```sh
$ python -m venv ENV
```

### 3.	Activate เข้าสู่ environment
```sh
$ source ENV/bin/activate
```

### 4. ติดตั้ง libraries ที่จำเป็น (ในครั้งแรก)
```sh
$ pip install -r requirements.txt
```

### 5. เตรียม code ในไฟล์ etl.py เพื่อนำเข้าข้อมูล load และ transform

### 6. เตรียมติดตั้งและเปิด Docker
```sh
$ mkdir -p ./dags ./logs ./plugins
$ echo -e "AIRFLOW_UID=$(id -u)" > .env
$ docker-compose up
```

### 7.	เปิด Airflow Port 8080

### 8. เข้า DAGs ที่ชื่อว่า Capstone 

### 9. สร้าง connection 
- Connection id: my-redshift 
- Connection Type: Redshift
- Host: มาจาก Endpoint บน Redshift
- Schema: dev 
- Login:  awsuser 
- Password คือ Password ที่ตั้งตอนสร้าง Redshift 
- Port: 5439

### 10.	RUN DAGs ชื่อ “Capstone”
![Airflow](https://user-images.githubusercontent.com/112712486/208810042-a1e53ab3-5619-4273-aa30-bdda9467bd1c.jpg)

## Cloud access AWS
### 1. หลังทำการ ETL ข้อมูลเรียบร้อยแล้ว ให้เข้าไปที่ Redshift เพื่อเชื่อมต่อกับ Database 
### 2. ทำการ Export CSV Files ออกมาเพื่อนำไปใช้งานต่อไป   

## Github
### 1.	Shutdown Docker ปิดการทำงานของ Docker
```sh
$ docker-compose down
```
### 2.	Deactivate the virtual environment
```sh
$ deactivate
```

## Data Visualization by PowerBI
![powerbi](https://user-images.githubusercontent.com/112712486/208810548-deb5c506-9649-4b37-9796-7f19b16aaae7.jpg)
