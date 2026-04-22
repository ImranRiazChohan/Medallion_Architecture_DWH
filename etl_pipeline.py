from bronze_layer import main as bronze_main
from silver_layer import load_silver as silver_main
from golden_layer import main as golden_main


def main():
    try:
        bronze_main()
        print("Bronze layer ingestion completed successfully.")
    except Exception as e:
        print(f"Error during bronze layer ingestion: {e}")
        raise

    try:
        silver_main()
        print("Silver layer transformation completed successfully.")
    except Exception as e:
        print(f"Error during silver layer transformation: {e}")
        raise

    try:
        golden_main()
        print("Golden layer aggregation completed successfully.")
    except Exception as e:
        print(f"Error during golden layer aggregation: {e}")
        raise

if __name__=='__main__':
    main()