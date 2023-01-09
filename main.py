import sys
from classes.exportData import exportData
from classes.loader import Loader

# Load Utilities

loader = Loader()

# Load providers

moralisProvider = exportData.Moralis()

# Main
## python main.py <provider> <function> <param1> <param2> ... <paramN>

if __name__ == "__main__":
    loader.start()
    if sys.argv[1] == 'moralis':
        moralisProvider = exportData.Moralis()
        if(sys.argv[2] == "getCollectionbyWalletAddress"):
            # python main.py moralis getCollectionbyWalletAddress <walletAddress> <outputFileName> <fileType : csv|xls|html|xml>
            moralisProvider.getCollectionByWalletAddress(sys.argv[3], sys.argv[4], sys.argv[5]) 
        elif(sys.argv[2] == "getNFTOwnersByContract"):
            # python main.py moralis getCollectionbyWalletAddress <smartContractAddress> <outputFileName> <fileType : csv|xls|html|xml>
            moralisProvider.getNFTOwnersByContract(sys.argv[3], sys.argv[4], sys.argv[5]) 
        elif(sys.argv[2] == "getNFTTransferByContract"):
            # python main.py moralis getCollectionbyWalletAddress <smartContractAddress> <outputFileName> <fileType : csv|xls|html|xml>
            moralisProvider.getNFTOwnersByContract(sys.argv[3], sys.argv[4], sys.argv[5]) 
        else:
            print("Function Not Found")
    else:
        print("Invalid argument")
    loader.stop()