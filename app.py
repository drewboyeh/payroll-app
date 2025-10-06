import streamlit as st
import pandas as pd
import io
from datetime import datetime

from local_payroll_analyzer import LocalPayrollAnalyzer


st.set_page_config(page_title="Payroll Analyzer", page_icon="🧮", layout="wide")
st.title("Jersey Mike’s Payroll Hours Proportion Analyzer")
st.write("Upload the three data files, then run the analysis and download the CSV.")


def read_pipe_txt(file):
    # Read raw bytes once
    try:
        file.seek(0)
    except Exception:
        pass
    raw = file.read()
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            text = raw.decode(enc)
            buf = io.StringIO(text)
            df = pd.read_csv(buf, sep="|", engine="python", on_bad_lines="skip")
            df.columns = df.columns.str.strip()
            return df
        except Exception as e:
            last_err = e
            continue
    # Final fallback: decode with cp1252 ignoring errors
    try:
        text = raw.decode("cp1252", errors="ignore")
        buf = io.StringIO(text)
        df = pd.read_csv(buf, sep="|", engine="python", on_bad_lines="skip")
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        raise last_err


with st.sidebar:
    st.header("Data Files")
    uploaded_time = st.file_uploader("Employee_Time_Clock .txt", type=["txt"], key="time")
    uploaded_employee = st.file_uploader("Employee.txt", type=["txt"], key="employee")
    uploaded_store = st.file_uploader("Store.txt", type=["txt"], key="store")

    use_custom_dates = st.checkbox("Specify custom date range", value=False)
    start_date = None
    end_date = None
    if use_custom_dates:
        start_date = st.date_input("Start date")
        end_date = st.date_input("End date")

    run = st.button("Run Analysis")


results_container = st.container()

if run:
    if not (uploaded_time and uploaded_employee and uploaded_store):
        st.error("Please upload all three files.")
    else:
        analyzer = LocalPayrollAnalyzer(data_folder_path=".")
        try:
            analyzer.employee_time_data = read_pipe_txt(uploaded_time)
            analyzer.employee_data = read_pipe_txt(uploaded_employee)
            analyzer.store_data = read_pipe_txt(uploaded_store)

            sd = pd.to_datetime(start_date) if start_date else None
            ed = pd.to_datetime(end_date) if end_date else None

            results = analyzer.analyze_pay_period(sd, ed)
            if results is None or len(results) == 0:
                st.warning("No results for the selected period.")
            else:
                st.success("Analysis complete.")
                with results_container:
                    st.dataframe(results)
                    missing_names = results[['Employee_ID']].copy()
                    if 'First_Name' in results.columns:
                        missing_names['First_Name'] = results['First_Name']
                    if 'Last_Name' in results.columns:
                        missing_names['Last_Name'] = results['Last_Name']
                    if 'First_Name' in missing_names.columns or 'Last_Name' in missing_names.columns:
                        null_mask = (
                            (missing_names['First_Name'].isna() if 'First_Name' in missing_names.columns else True) &
                            (missing_names['Last_Name'].isna() if 'Last_Name' in missing_names.columns else True)
                        )
                        count_missing = int(null_mask.sum())
                        if count_missing > 0:
                            st.info(f"{count_missing} employees missing names; showing IDs only.")

                # Build CSV into buffer using analyzer's save_report which supports file-like
                csv_buf = io.StringIO()
                analyzer.save_report(results, filename=csv_buf)
                csv_bytes = csv_buf.getvalue().encode("utf-8")
                st.download_button(
                    label="Download CSV",
                    data=csv_bytes,
                    file_name="payroll_analysis.csv",
                    mime="text/csv",
                )
        except Exception as e:
            st.error(f"Error running analysis: {e}")


