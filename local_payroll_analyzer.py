import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class LocalPayrollAnalyzer:
    def __init__(self, data_folder_path):
        self.data_folder = data_folder_path
        self.employee_time_data = None
        self.employee_data = None
        self.store_data = None

    def calculate_hours_worked(self, start_time, end_time):
        if pd.isna(start_time) or pd.isna(end_time):
            return 0
        try:
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
            if end_dt < start_dt:
                end_dt += timedelta(days=1)
            hours = (end_dt - start_dt).total_seconds() / 3600
            return min(hours, 16) if hours > 0 else 0
        except Exception:
            return 0

    def get_pay_period_dates(self, reference_date=None):
        if reference_date is None:
            reference_date = datetime.now()
        elif isinstance(reference_date, str):
            reference_date = pd.to_datetime(reference_date)
        days_since_sunday = (reference_date.weekday() + 1) % 7
        if days_since_sunday == 0:
            days_since_sunday = 7
        period_end = reference_date - timedelta(days=days_since_sunday)
        period_end = period_end.replace(hour=23, minute=59, second=59)
        period_start = period_end - timedelta(days=13)
        period_start = period_start.replace(hour=0, minute=0, second=0)
        return period_start, period_end

    def analyze_pay_period(self, start_date=None, end_date=None):
        if self.employee_time_data is None:
            return None
        if start_date is None or end_date is None:
            start_date, end_date = self.get_pay_period_dates()
        else:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)

        time_data = self.employee_time_data.copy()
        time_data['Start'] = pd.to_datetime(time_data['Start'], errors='coerce')
        time_data['End'] = pd.to_datetime(time_data['End'], errors='coerce')
        period_mask = (
            (time_data['Start'] >= start_date) &
            (time_data['Start'] <= end_date) &
            (time_data['Start'].notna()) &
            (time_data['End'].notna())
        )
        period_data = time_data[period_mask].copy()
        if len(period_data) == 0:
            return None
        period_data['Hours_Worked'] = period_data.apply(
            lambda row: self.calculate_hours_worked(row['Start'], row['End']),
            axis=1
        )
        period_data = period_data[period_data['Hours_Worked'] > 0]
        employee_hours = period_data.groupby(['Store_ID', 'Employee_ID'])['Hours_Worked'].sum().reset_index()
        store_totals = employee_hours.groupby('Store_ID')['Hours_Worked'].sum().reset_index()
        store_totals.columns = ['Store_ID', 'Total_Store_Hours']
        results = employee_hours.merge(store_totals, on='Store_ID')
        results['Hours_Proportion'] = results['Hours_Worked'] / results['Total_Store_Hours']
        results['Hours_Percentage'] = results['Hours_Proportion'] * 100
        if self.employee_data is not None:
            employee_info = self.employee_data[['Employee_ID', 'First_Name', 'Last_Name', 'Store_ID']].copy()
            results = results.merge(employee_info, on=['Employee_ID', 'Store_ID'], how='left')
        if self.store_data is not None:
            store_info = self.store_data[['Store_ID', 'Store_Number', 'Store_Name']].drop_duplicates()
            results = results.merge(store_info, on='Store_ID', how='left')
        results = results.sort_values(['Store_ID', 'Hours_Worked'], ascending=[True, False])
        return results

    def save_report(self, results, filename="payroll_proportions.csv"):
        if results is None:
            return
        output_cols = ['Store_ID']
        if 'Store_Number' in results.columns:
            output_cols.append('Store_Number')
        if 'Store_Name' in results.columns:
            output_cols.append('Store_Name')
        output_cols.extend(['Employee_ID'])
        if 'First_Name' in results.columns:
            output_cols.append('First_Name')
        if 'Last_Name' in results.columns:
            output_cols.append('Last_Name')
        output_cols.extend(['Hours_Worked', 'Total_Store_Hours', 'Hours_Proportion', 'Hours_Percentage'])
        available_cols = [col for col in output_cols if col in results.columns]
        if hasattr(filename, "write"):
            results[available_cols].to_csv(filename, index=False)
        else:
            results[available_cols].to_csv(filename, index=False)


