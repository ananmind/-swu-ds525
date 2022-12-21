## Project instruction
[Project Instruction.pdf](https://github.com/ananmind/-swu-ds525/files/10274055/Project.Instruction.pdf)
<br>

## Project presentation
[Slide Capstone Project.pdf](https://github.com/ananmind/-swu-ds525/files/10273958/Slide.Capstone.Project.pdf)
<br>

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

