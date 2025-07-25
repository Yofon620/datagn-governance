import asyncio
from datetime import datetime
from dao.interface_repo import InterfaceRepo
from dao.metric_repo import MetricRepo
from dao.business_repo import BusinessLevelRepo
from dao.quality_repo import QualityRepo
from dao.scale_repo import ScaleRepo
from service.interface_service import InterfaceService
from service.metric_service import MetricService
from service.business_level_service import BusinessLevelService
from service.quality_service import QualityService
from service.scale_service import ScaleService

"""
| 层级                  | 目录/模块   | 主要职责                        
| -----------------    | ----------| ---------------------------    
| **配置层**            | `config`  | 统一读取环境变量、数据库连接串           
| **数据访问层 DAO**     | `dao`     | 只负责 **读写数据库**，不掺杂任何业务逻辑     
| **业务逻辑层 Service** | `service` | 只负责 **算法/计算**，不感知数据存储细节     
| **公共工具层 Utils**   | `common`  | 只负责 **公共函数**
| **入口/编排层**        | `main.py` | **选择/编排** 调用哪个 Service，控制启停            
"""


"""
interface_service  → data_fabric_interface_detail.csv    ---驾驶舱接口明细表
        ↓
metric_service     → data_fabric_metric_trend.csv（日/周/月滚动 7+30 天）    ---驾驶舱接口趋势表
        ↓
business_service   → data_fabric_interface_business_level.csv（stability/timeliness/...）    ---驾驶舱接口业务级别表
        ↓ 拆分
quality_service    → data_fabric_interface_quality.csv（stability_ratio / timeliness_ratio）    ---驾驶舱接口质量表
scale_service      → data_fabric_interface_scale.csv   ---驾驶舱接口规模表
"""

async def run_detail(date_str: str = datetime.now().strftime('%Y-%m-%d')):
    """
    :param date_str: 数据日期
    :return: dataframe
    """
    repo = InterfaceRepo()
    svc = InterfaceService(repo)
    df = await svc.build_detail(date_str)
    await repo.write_detail(df)
    print("✅ 接口明细已写入")
    await repo.engine.dispose()

async def run_metric(date_str: str = datetime.now().strftime('%Y%m%d')):
    """
    :param date_str: 数据录入日期，注意：不同于数据日期！
    :return: dataframe
    """
    repo = MetricRepo()
    svc = MetricService(repo)
    metric_df = await svc.build_metric(date_str)
    await repo.write_metric(metric_df)
    print("✅ 指标已写入")
    await repo.engine.dispose()

async def run_business_level(date_str: str = datetime.now().strftime('%Y%m%d')):
    """
    :param date_str: 数据录入日期，注意：不同于数据日期！
    :return: dataframe
    """
    repo = BusinessLevelRepo()
    agg = BusinessLevelService(repo)
    business_level_df = await agg.build_aggregate(date_str)
    await repo.write_data(business_level_df)
    print("✅ 业务级数据已生成")
    await repo.engine.dispose()

async def run_quality(date_str: str = datetime.now().strftime('%Y%m%d')):
    repo = QualityRepo()
    svc = QualityService(repo)
    df = await svc.build_quality(date_str)
    await repo.write_quality(df)
    print("✅ 驾驶舱接口质量规模表已生成并入库")
    await repo.engine.dispose()

async def run_scale(date_str: str = datetime.now().strftime('%Y%m%d')):
    repo = ScaleRepo()
    svc = ScaleService(repo)
    df = await svc.build_scale(date_str)
    await repo.write_scale(df)
    print("✅ 驾驶舱接口质量规模表已生成并入库")
    await repo.engine.dispose()

async def run_all():
    # await run_detail('2025-07-22')
    # await run_metric('20250720')
    # await run_business_level('20250720')
    # await run_quality('20250721')
    await run_scale('20250721')

if __name__ == "__main__":
    asyncio.run(run_all())

