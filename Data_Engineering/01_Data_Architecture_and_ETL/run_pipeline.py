import subprocess
import time
import sys

PIPELINE_SCRIPTS = [
    "get_whole_Alberta_owner_sales_car_in_kijiji.py",
    "get_details.py",
    "clean_sold_data.py",
    "Clean1.py",
    "Clean2.py"]
def run_pipeline():
    print("=" * 60)
    print("\n [START] Alberta Car Sales Data Pipeline")
    print("=" * 60) 
    start_time = time.time()
    for script in PIPELINE_SCRIPTS:
        print(f"\n [RUNNING] {script} ...")
        try:
            result = subprocess.run([sys.executable, script], check=True)
            print(f"\n [SUCCESS] {script} is over！")
        
        except subprocess.CalledProcessError as e:
            print(f"\n [ERROR] {script} something wrong！")
            sys.exit(1)
            
    end_time = time.time()
    elapsed = (end_time - start_time) / 60
    
    print("\n" + "=" * 60)
    print(f"\n Mission complete")
    print(f"\n Spend time: {elapsed:.2f} min")
    print(f"\n Save as ‘Optimized_Alberta_owner_sales_car_clean.csv’")
    print("=" * 60)

if __name__ == "__main__":
    run_pipeline()