import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path

class LocalPayrollAnalyzer:
    def __init__(self, data_folder_path):
        self.data_folder = data_folder_path
        self.employee_time_data = None
        self.employee_data = None
        self.store_data = None
    
    def load_data_files(self):
        """Load the required Jersey Mike's data files"""
        print(f"Looking for files in: {self.data_folder}")
        
        # Find all .txt files in the folder
        txt_files = []
        for root, dirs, files in os.walk(self.data_folder):
            for file in files:
                if file.endswith('.txt'):
                    txt_files.append(os.path.join(root, file))
        
        print(f"Found {len(txt_files)} text files")
        
        # Load the specific files we need
        for file_path in txt_files:
            filename = os.path.basename(file_path).lower()
            
            try:
                if 'employee_time_clock' in filename:
                    print(f"Loading: {filename}")
                    self.employee_time_data = pd.read_csv(file_path, sep='|', encoding='utf-8')
                    self.employee_time_data.columns = self.employee_time_data.columns.str.strip()
                    print(f"  - Loaded {len(self.employee_time_data)} time clock records")
                
                elif filename == 'employee.txt':
                    print(f"Loading: {filename}")
                    self.employee_data = pd.read_csv(file_path, sep='|', encoding='utf-8')
                    self.employee_data.columns = self.employee_data.columns.str.strip()
                    print(f"  - Loaded {len(self.employee_data)} employee records")
                
                elif filename == 'store.txt':
                    print(f"Loading: {filename}")
                    self.store_data = pd.read_csv(file_path, sep='|', encoding='utf-8')
                    self.store_data.columns = self.store_data.columns.str.strip()
                    print(f"  - Loaded {len(self.store_data)} store records")
            
            except Exception as e:
                print(f"Error loading {filename}: {e}")
        
        # Check what we successfully loaded
        success = True
        if self.employee_time_data is None:
            print("❌ Employee_Time_Clock.txt not found or failed to load")
            success = False
        if self.employee_data is None:
            print("❌ Employee.txt not found or failed to load") 
            success = False
        if self.store_data is None:
            print("❌ Store.txt not found or failed to load")
            success = False
        
        if success:
            print("✅ All required files loaded successfully")
        
        return success
    
    def calculate_hours_worked(self, start_time, end_time):
        """Calculate hours between start and end times"""
        if pd.isna(start_time) or pd.isna(end_time):
            return 0
        
        try:
            start_dt = pd.to_datetime(start_time)
            end_dt = pd.to_datetime(end_time)
            
            # Handle overnight shifts
            if end_dt < start_dt:
                end_dt += timedelta(days=1)
            
            duration = end_dt - start_dt
            hours = duration.total_seconds() / 3600
            
            # Cap at 16 hours max per shift
            return min(hours, 16) if hours > 0 else 0
            
        except Exception as e:
            return 0
    
    def get_pay_period_dates(self, reference_date=None):
        """Get start and end dates for a 2-week pay period ending before reference date"""
        if reference_date is None:
            reference_date = datetime.now()
        elif isinstance(reference_date, str):
            reference_date = pd.to_datetime(reference_date)
        
        # Find most recent Sunday before reference date
        days_since_sunday = (reference_date.weekday() + 1) % 7
        if days_since_sunday == 0:  # If it's Sunday
            days_since_sunday = 7
        
        period_end = reference_date - timedelta(days=days_since_sunday)
        period_end = period_end.replace(hour=23, minute=59, second=59)
        
        period_start = period_end - timedelta(days=13)  # 14 days total
        period_start = period_start.replace(hour=0, minute=0, second=0)
        
        return period_start, period_end
    
    def analyze_pay_period(self, start_date=None, end_date=None):
        """Calculate employee hour proportions by store for a pay period"""
        
        if self.employee_time_data is None:
            print("No employee time clock data available")
            return None
        
        # Set pay period dates
        if start_date is None or end_date is None:
            start_date, end_date = self.get_pay_period_dates()
        else:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
        
        print(f"\nAnalyzing pay period: {start_date.date()} to {end_date.date()}")
        
        # Prepare time clock data
        time_data = self.employee_time_data.copy()
        
        # Convert date columns
        time_data['Start'] = pd.to_datetime(time_data['Start'], errors='coerce')
        time_data['End'] = pd.to_datetime(time_data['End'], errors='coerce')
        
        # Filter for pay period
        period_mask = (
            (time_data['Start'] >= start_date) & 
            (time_data['Start'] <= end_date) &
            (time_data['Start'].notna()) &
            (time_data['End'].notna())
        )
        
        period_data = time_data[period_mask].copy()
        
        if len(period_data) == 0:
            print("No time clock data found for this pay period")
            return None
        
        print(f"Found {len(period_data)} time clock records in pay period")
        
        # Calculate hours for each shift
        period_data['Hours_Worked'] = period_data.apply(
            lambda row: self.calculate_hours_worked(row['Start'], row['End']), 
            axis=1
        )
        
        # Remove shifts with 0 hours
        period_data = period_data[period_data['Hours_Worked'] > 0]
        
        # Group by Store and Employee to get total hours
        employee_hours = period_data.groupby(['Store_ID', 'Employee_ID'])['Hours_Worked'].sum().reset_index()
        
        # Calculate total hours per store
        store_totals = employee_hours.groupby('Store_ID')['_Hours_Worked'].sum().reset_index() if False else employee_hours.groupby('Store_ID')['Hours_Worked'].sum().reset_index()
        store_totals.columns = ['Store_ID', 'Total_Store_Hours']
        
        # Merge to calculate proportions
        results = employee_hours.merge(store_totals, on='Store_ID')
        results['Hours_Proportion'] = results['Hours_Worked'] / results['Total_Store_Hours']
        results['Hours_Percentage'] = results['Hours_Proportion'] * 100
        
        # Add employee names if available
        if self.employee_data is not None:
            employee_info = self.employee_data[['Employee_ID', 'First_Name', 'Last_Name', 'Store_ID']].copy()
            results = results.merge(employee_info, on=['Employee_ID', 'Store_ID'], how='left')
        
        # Add store information if available
        if self.store_data is not None:
            store_info = self.store_data[['Store_ID', 'Store_Number', 'Store_Name']].drop_duplicates()
            results = results.merge(store_info, on='Store_ID', how='left')
        
        # Sort by store, then by hours (highest first)
        results = results.sort_values(['Store_ID', 'Hours_Worked'], ascending=[True, False])
        
        print(f"✅ Calculated proportions for {len(results)} employee-store combinations")
        print(f"Stores covered: {results['Store_ID'].nunique()}")
        print(f"Total employees: {results['Employee_ID'].nunique()}")
        
        return results
    
    def save_report(self, results, filename="payroll_proportions.csv"):
        """Save results to CSV file"""
        if results is None:
            print("No results to save")
            return
        
        # Select and order columns for output
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
            
        output_cols.extend([
            'Hours_Worked', 
            'Total_Store_Hours', 
            'Hours_Proportion', 
            'Hours_Percentage'
        ])
        
        # Filter to only columns that exist
        available_cols = [col for col in output_cols if col in results.columns]
        
        # Save to CSV
        results[available_cols].to_csv(filename, index=False)
        print(f"✅ Report saved to: {filename}")
        
        # Print summary
        print("\n" + "="*50)
        print("PAYROLL ANALYSIS SUMMARY")
        print("="*50)
        
        for store_id in sorted(results['Store_ID'].unique()):
            store_data = results[results['Store_ID'] == store_id]
            
            store_name = ""
            if 'Store_Number' in store_data.columns:
                store_num = store_data['Store_Number'].iloc[0]
                store_name = f" (#{store_num})"
            if 'Store_Name' in store_data.columns:
                name = store_data['Store_Name'].iloc[0]
                if pd.notna(name):
                    store_name += f" - {name}"
            
            total_hours = store_data['Total_Store_Hours'].iloc[0]
            employee_count = len(store_data)
            
            print(f"\nStore {store_id}{store_name}:")
            print(f"  Total Hours: {total_hours:.1f}")
            print(f"  Employees: {employee_count}")
            print(f"  Top Contributors:")
            
            top_employees = store_data.head(3)
            for _, emp in top_employees.iterrows():
                name = ""
                if 'First_Name' in emp and pd.notna(emp['First_Name']):
                    name = f" ({emp['First_Name']})"
                print(f"    Employee {emp['Employee_ID']}{name}: {emp['Hours_Worked']:.1f} hrs ({emp['Hours_Percentage']:.1f}%)")

# Simple usage function
def analyze_payroll(data_folder, start_date=None, end_date=None, output_file="payroll_analysis.csv"):
    """
    Simple function to analyze payroll data
    
    Args:
        data_folder: Path to folder containing Jersey Mike's .txt files
        start_date: Start date for analysis (optional, defaults to last 2-week period)
        end_date: End date for analysis (optional)
        output_file: Name for output CSV file
    """
    
    print("Jersey Mike's Payroll Hours Proportion Analysis")
    print("=" * 50)
    
    # Initialize analyzer
    analyzer = LocalPayrollAnalyzer(data_folder)
    
    # Load data files
    if not analyzer.load_data_files():
        print("Failed to load required data files")
        return None
    
    # Run analysis
    results = analyzer.analyze_pay_period(start_date, end_date)
    
    if results is not None:
        # Save report
        analyzer.save_report(results, output_file)
        return results
    else:
        print("Analysis failed")
        return None

# Example usage
if __name__ == "__main__":
    # CHANGE THIS to your actual folder path
    DATA_FOLDER = "/Users/andrewyeh/Downloads"  # Your actual path
    
    # Run the analysis with your actual data
    results = analyze_payroll(DATA_FOLDER)
    
    # Run the analysis
    results = analyze_payroll(DATA_FOLDER)
    
    if results is not None:
        print(f"\n✅ Analysis complete! Check your CSV file for detailed results.")
        print(f"Tip distribution can be calculated using the 'Hours_Proportion' column.")


