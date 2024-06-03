import sys
import os

# Lấy đường dẫn tuyệt đối của thư mục hiện tại (dags)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Thêm đường dẫn của thư mục chứa config.py vào sys.path
config_dir = os.path.join(current_dir, '..', 'scripts')
sys.path.append('/Users/minhtuyen02/MTuyen/work-space/University/doantotnghiep_2024/Graduation_Thesis_SOICT_2024/docker-custom-service/airflow/scripts')
from spark.gold_fact_fhvhv_tracking import main
main()