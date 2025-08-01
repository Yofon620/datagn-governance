import re
import time
import numpy as np
import pandas as pd
from datetime import datetime
from dao.interface_repo import InterfaceRepo

# ---------- 常量 ----------
STAGE_DICT = pd.DataFrame(
    [('10', '扫描'), ('01', '扫描'), ('20', '分拣'), ('40', '清洗'),
     ('50', '转换'), ('52', '分发'), ('60', '入库'), ('95', '入库'),
     ('80', '校验'), ('99', '校验')],
    columns=['stage_code', 'stage_name']
)

STAGE_MAP = {
    '扫描': 'scan', '分拣': 'sorting', '清洗': 'cleaning', '分发': 'distribute',
    '转换': 'conversion', '入库': 'warehousing', '校验': 'inspection',
}
PT_PREFIX_LEN = {'省经': 2, '一经': 2}   # 其它平台 3


class InterfaceService:
    def __init__(self, repo: InterfaceRepo):
        self.repo = repo

    def split_platform_interface(self, df: pd.DataFrame) -> pd.DataFrame:
        """初步拆分平台和接口"""
        df = df.assign(storage_interface_id=lambda x: x['storage_interface_id'].astype(str).str.replace(' ', ''))
        split1 = df.assign(platform_block=lambda x: x['storage_interface_id'].str.split('|')).explode(
            'platform_block').query("platform_block != ''")
        split2 = split1.assign(platform_block=lambda x: x['platform_block'].str.split(r'[，,、]')).explode(
            'platform_block').query("platform_block != ''")

        def parse_one(s):
            s = str(s).strip()
            if s.startswith('云平台'):
                return '云平台', s[3:]
            elif s.startswith('省经'):
                return '省经', s[2:]
            elif s.startswith('一经'):
                return '一经', s[2:]
            else:
                return None, None

        parsed = split2.assign(
            pt=lambda x: x['platform_block'].apply(lambda v: parse_one(v)[0]),
            storage_interface_id=lambda x: x['platform_block'].apply(lambda v: parse_one(v)[1])
        ).dropna(subset=['storage_interface_id']).assign(
            storage_interface_id=lambda x: x['storage_interface_id'].str.zfill(4))
        parsed.drop(columns=['platform_block'])
        print(f"拆分平台后共有{len(parsed)}个记录")
        return parsed

    async def build_detail(self, date_str: str) -> pd.DataFrame:

        meta = await self.repo.load_meta_data_interface()
        tmp = (
            meta
            .assign(
                interface_id=lambda x: x['interface_id'],
                department=lambda x: x['responsibility_department'],
                # create_time=datetime.now().strftime('%Y%m%d'),
                create_time=20250627,
                # statistic_cycle=lambda x: x['collection_cycle'].map({
                #     '日': 1,
                #     '周': 2,
                #     '月': 3
                # }),
                # statistic_cycle='-',  # 统计类型
                # metric_type='-',
                interface_name=lambda x: x['interface_name'],
                interface_file_name=lambda x: x['model_name_en'],
                # storage_system=lambda x: x['pt'],  # 存储系统
                storage_interface_id=lambda x: x['interface_storage_id'],  # 存储系统接口编号(4位数)
                # biz_name=lambda x: x['importance_level'].map({
                #     'P0': '一经', 'P1': '掌经', 'P2': '大音',
                #     'P3': '重点监控', 'P4': '移动办公常关注', 'P5': '其他'
                # }),
                biz_name=lambda x: x['business_scene'].str.replace('相关', ''),
                type=lambda x: x['interface_type'],
                level=lambda x: x['importance_level'],
                protocol_upload_time=lambda x: x['interface_data_scheduled_arrival_time']
            )
        )[['interface_id', 'department', 'create_time',
           'interface_name', 'interface_file_name',
           'storage_interface_id', 'type', 'level',
           'protocol_upload_time', 'biz_name']]

        print(f"源接口资源表共{len(tmp)}个记录")
        sql_df = self.split_platform_interface(tmp)

        reg = await self.repo.load_register()
        error = await self.repo.load_error(date_str)
        delay = await self.repo.load_delay(date_str)
        print(f"数智运维平台 - register:{len(reg)} | error:{len(error)} | delay:{len(delay)}")

        def prefix_len(pt):
            return 2 if pt in ('省经', '一经') else 3

        # 此部分处理为同步处理,dataframe在内存中无异步操作
        rows = []
        for r in reg.itertuples(index=False):
            stage_raw = str(r.job_stage).strip()
            pt_val = r.pt
            if stage_raw in ('', 'null', 'None'):
                stage_raw = '01' if pt_val in ('省经', '一经') else '10'
            else:
                stage_raw = stage_raw.replace('null', '10')
            for stage_num in stage_raw.split(','):
                stage_num = stage_num.strip()
                if not stage_num:
                    continue
                new_job_id = (
                        r.job_id[:prefix_len(pt_val)] +
                        str(r.interface_id).zfill(4) +
                        stage_num.zfill(2)
                )
                rows.append({
                    'new_job_id': new_job_id,
                    'interface_id': str(r.interface_id).zfill(4),
                    'pt': pt_val,
                    'stage_num': stage_num
                })

        uni_df = pd.DataFrame(rows)

        uni = uni_df.drop_duplicates(['pt', 'new_job_id'])
        uni = uni.merge(STAGE_DICT, left_on='stage_num', right_on='stage_code', how='left').fillna({'stage_name': '未知'})
        print(f"拼接ETL作业号后共有{len(uni)}个")

        err_set = set(error['job_id'])
        dly_set = set(delay['job_id'])
        uni['failure_flag'] = uni['new_job_id'].isin(err_set)
        uni['delay_flag'] = uni['new_job_id'].isin(dly_set)

        flag = (
            uni.groupby(['interface_id', 'pt', 'stage_name'], as_index=False).agg(has_fail=('failure_flag', 'any'), has_delay=('delay_flag', 'any'))
        )
        flag['has_fail'] = flag['has_fail'].map({True: '100%', False: '0%'})
        flag['has_delay'] = flag['has_delay'].map({True: '0%', False: '100%'})  # 统计的是及时率，从延迟表里来，就

        flag['stage_name_en'] = (
            flag['stage_name']
            .map(STAGE_MAP)
            .fillna(
                flag['stage_name']
                .mask(flag['stage_name'].isin(STAGE_MAP.keys()), '')
                .replace('', pd.NA)
                .dropna()
                .astype('category')
                .cat.codes.add(1)
                .astype(str)
                .radd('other')
                )
        )
        print(f"按接口编号、平台、ETL作业名分组后记录数{len(flag)}个")
        flag['f_col'] = flag['stage_name_en'] + '_failure_rate'
        flag['d_col'] = flag['stage_name_en'] + '_timeliness_rate'
        # 以interface_id和pt作为索引，生成透视表
        pivot_f = (flag.pivot(index=['interface_id', 'pt'], columns='f_col', values='has_fail').fillna(False).reset_index())
        pivot_d = (flag.pivot(index=['interface_id', 'pt'], columns='d_col', values='has_delay').fillna(False).reset_index())
        # 合并两个透视表
        pivot = pd.merge(pivot_f, pivot_d, on=['interface_id', 'pt'], how='outer').fillna(False)

        for col in pivot.columns:
            pivot[col] = pivot[col].replace({True: '100%', False: '0%'})

        final_tmp = sql_df.merge(
            pivot,
            left_on=['storage_interface_id', 'pt'],
            right_on=['interface_id', 'pt'],
            how='right'
        )
        # final_tmp.to_csv('final_tmp_join_result.csv', index=False, encoding='utf_8_sig')
        base_ms = int(time.time() * 1000)
        # meta['interface_detail_id'] = [str(base_ms + i) for i in range(len(meta))]
        final = final_tmp.assign(
            interface_detail_id=lambda x: (base_ms + np.arange(len(x))).astype(str),  # 主键ID
            data_date=date_str,
            interface_id=lambda x: x['interface_id_x'],  # 治理平台的六位数接口编号
            interface_id_op=lambda d: d['interface_id_y'],  # 运维平台的四位数接口号，用于后续匹配
            storage_system=lambda x: x['pt'].map({
                '云平台': 'HADOOP',
                '一经': 'ORACLE',
                '省经': 'MPP'
            }).fillna('-'),  # 存储系统
            protocol_field_count=0,  # 协议字段个数
            file_field_count=0,  # 文件字段个数
            file_field_completeness_rate='100%',  # 文件字段完整率
            sampling_field_accuracy='100%',
            primary_key_uniqueness_rate='100%',
            file_record_count=0,
            warehousing_record_count=0,
            record_count_consistency_rate='100%',
            inspected_fields='NULL',
            field_format_normativity_rate='100%',
            operation_stability=lambda d: (
                (100 - d.filter(regex=r'_failure_rate$')
                 .apply(lambda s: pd.to_numeric(s.str.rstrip('%'), errors='coerce'))
                 .mean(axis=1))
                .fillna(0).astype(int).astype(str) + '%'
            ),
            scan_arrival_time='-',  # 扫描作业到达时间
            cleaning_avg_time='-',  # 平均清洗时间
            cleaning_arrival_time='-',  # 清洗作业到达时间
            conversion_avg_time='-',  # 平均转换时间
            conversion_arrival_time='-',  # 转换作业到达时间
            sorting_avg_time='-',  # 平均分拣时间
            sorting_arrival_time='-',  # 分拣作业到达时间
            distribute_avg_time='-',  # 平均分发时间
            distribute_arrival_time='-',  # 分发作业到达时间
            warehousing_avg_time='-',  # 平均入库时间
            warehousing_arrival_time='-',  # 入库作业到达时间
            inspection_avg_time='-',  # 平均校验时间
            inspection_arrival_time='-'  # 校验作业到达时间
        )

        # 避免主键重复
        final = final.drop_duplicates(subset=['storage_interface_id', 'pt', 'data_date'])

        rate_cols = [c for c in final.columns if c.endswith(('_failure_rate', '_timeliness_rate'))]
        final[rate_cols] = final[rate_cols].fillna('100%')
        final = final.drop(columns=[c for c in final.columns if c.endswith(('_x', '_y', '_csv'))])

        final = final.drop(columns=['platform_block'])
        print(f"最终右关联数智运维平台业务表后记录数{len(final)}个")
        final.to_csv(f"data_fabric_interface_detail.csv", index=False, encoding='utf_8_sig')

        return final