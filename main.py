from etl_source_to_OLTP import source_to_OLTP
# from etl_OLTP_to_OLAP import OLTP_to_OLAP

def main():
    source_to_OLTP()
    # OLTP_to_OLAP()

if __name__ == '__main__':
    main()