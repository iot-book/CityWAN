from .dev_anal import gtw2dev

def main():
    # suppress_qt_warnings()
    # homePath = r'F:/TCloud/Seafile/shuaiFile/InProgress/2023_Experience/dataset/raw_data/'
    srcPath = r'F:/Home/dataset/'
    homePath = r'F:/Home/data_out/'
    # sort_by_gtw(srcPath, homePath+'sort_by_gateway/')
    # sort_by_dev(srcPath, homePath+'sort_by_service/')
    # devEUIdict, devEUIval = read_values(inPath)
    # sort_by_service(srcPath, homePath+'sort_by_service/')

    # pkt_loss(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/packet_loss.csv')
    # trans_anal(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/trans_analysis.csv')
    # snr_anal_dev(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/trans_snr.csv', homePath+'ENN_Result/dev_snr.csv')
    # dev_gtw_all(homePath+'sort_by_service/ENNGas/', homePath+'ENN_Result/trans_gtw_all.csv', homePath+'ENN_Result/dev_gtw_all.csv')
    # gtw_all(homePath+'sort_by_gtw/', homePath+'gtw_all.csv')
    gtw2dev(homePath+'sort_by_gateway/', homePath+'ENN_Result/packet_loss.csv', homePath+'ENN_Result/GTW2DEV/')

if __name__ == "__main__":
    main()