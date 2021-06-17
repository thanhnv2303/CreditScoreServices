## Running in Docker

1. Install Docker https://docs.docker.com/install/
2. Install Docker compose https://docs.docker.com/compose/install/
3. Build a docker image

        > docker-compose build 
4. Create file .env look for example at file example.env

5. Run a docker compose file 
   
        > docker-compose up


## Run for dev
   1. Install dependencies 
      > pip3 install -r requirements.txt
      
   2. Install Docker https://docs.docker.com/install/
   3. Install Docker compose https://docs.docker.com/compose/install/
   4. Create mongodb
      > cp example.env .env
      
      > docker-compose up neo4j-klg
      
   5. Quick run for dev
    
##### Run module aggrgate data for credit score
      
    >   python3 ./data_aggregation/test/run_stream.py    

##### Run module calculate credit score
      >  python3 ./calculate_credit_score/test/calculate_credit_score.py

##### Run module rest api for credit score
      >  python3 ./rest_api_server/main.py

### Note
Thư mục data_aggregation chứa các thành phần xử lý cho module tổng hợp dữ liệu.

Thư mục calculate_credit_score chứa các thành phần xử lý cho module tính điểm tín dụng.

Thư mục rest_api_server chứa các thành phần xử lý cho module api.

Các thư mục còn lại chứa những thư viện dùng chung, các file tiện ích, file cấu hình, hằng số và code mẫu.

Hiện tại các module data_aggregation và calculate_credit_score đang được đặt trong code mẫu xử lý theo streaming
và có thể sử dụng cấu trúc xử lý dữ liệu đa luồng (code mẫu nằm trong thư mục jobs).
Các nhà phát triển có thể tùy biến lại theo cách triển khai của mình.