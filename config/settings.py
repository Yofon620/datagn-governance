from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "yuyufeng"
    database: str = "mysql"
    charset: str = "utf8mb4"

    @property
    def url(self) -> str:
        return f"mysql+aiomysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset={self.charset}"


data_fabric_interface_detail_cols = [
    'interface_detail_id',
    'department',
    'create_time',
    'statistic_cycle',
    'metric_type',
    'interface_id',
    'interface_name',
    'interface_file_name',
    'storage_system',
    'storage_interface_id',
    'biz_name',
    'type',
    'level',
    'protocol_upload_time',
    'data_date',
    'scan_failure_rate',
    'sorting_failure_rate',
    'cleaning_failure_rate',
    'conversion_failure_rate',
    'warehousing_failure_rate',
    'distribute_failure_rate',
    'inspection_failure_rate',
    'operation_stability',
    'scan_arrival_time',
    'scan_timeliness_rate',
    'sorting_avg_time',
    'sorting_timeliness_rate',
    'sorting_arrival_time',
    'cleaning_avg_time',
    'cleaning_arrival_time',
    'cleaning_timeliness_rate',
    'conversion_avg_time',
    'conversion_arrival_time',
    'conversion_timeliness_rate',
    'distribute_timeliness_rate',
    'distribute_arrival_time',
    'distribute_avg_time',
    'warehousing_avg_time',
    'warehousing_arrival_time',
    'warehousing_timeliness_rate',
    'inspection_avg_time',
    'inspection_arrival_time',
    'inspection_timeliness_rate',
    'protocol_field_count',
    'file_field_count',
    'file_field_completeness_rate',
    'sampling_field_accuracy',
    'primary_key_uniqueness_rate',
    'file_record_count',
    'warehousing_record_count',
    'record_count_consistency_rate',
    'inspected_fields',
    'field_format_normativity_rate',
    'pt',
    'interface_id_op'
]

settings = Settings()