from clean_data import process_month

def main():
    raw_data_dir = 'data/raw_data'
    processed_data_dir = 'data/proc_data'
    
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    years = [2024, 2025]
    
    for y in years:
        for m in months:
            res = process_month(base_path=raw_data_dir, month=m, year=y, output_dir=processed_data_dir)
            if res is not None:
                print(res)

if __name__ == "__main__":
    main()



