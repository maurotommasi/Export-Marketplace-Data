import pandas as pd
import os
from moralis import evm_api
from classes.dotenv import dotEnv
from classes.database import MYSQL

class exportData:

    def __init__(self) -> None:
        pass

    class Moralis:

        def __init__(self) -> None:
            pass

        def __export(self, rawData, fileType, outputName):
            
            os.mkdir(dotEnv.EXPORT_FOLDER) if not os.path.exists(dotEnv.EXPORT_FOLDER) else True
            fileType = fileType.lower()
            if(fileType == "csv"):
                pd.DataFrame(rawData).to_csv("{}/{}.{}".format(dotEnv.EXPORT_FOLDER, outputName, fileType))
            elif(fileType == "xls"):
                pd.DataFrame(rawData).to_excel("{}/{}.{}".format(dotEnv.EXPORT_FOLDER, outputName, fileType))
            elif(fileType == "xml"):
                pd.DataFrame(rawData).to_xml("{}/{}.{}".format(dotEnv.EXPORT_FOLDER, outputName, fileType))
            elif(fileType == "html"):
                pd.DataFrame(rawData).to_html("{}/{}.{}".format(dotEnv.EXPORT_FOLDER, outputName, fileType))
            elif(fileType == "mysql"):
                MYSQL().insert(outputName, pd.DataFrame(rawData))
            else:
                print(rawData)
            print("Export successfull at {}/{}.{}".format(dotEnv.EXPORT_FOLDER, outputName, fileType)) if correctFormat else True

        def __getNestedParam(self, result, paramString):
            if(paramString.count(dotEnv.NESTED_SEPARATOR_CHAR) > 0):
                paramStringSplit = paramString.split(dotEnv.NESTED_SEPARATOR_CHAR,1) 
                left = paramStringSplit[0]
                right = paramStringSplit[1]
                value = self.__getNestedParam(result[left], right)
            else:
                value = result[paramString]
            return value

        def __getDataFromMoralis(self, params, function, rawData):
            api_key = dotEnv.API_KEY_MORALIS
            results = function(
                api_key=api_key,
                params=params,
            )
            params["cursor"] = results["cursor"]
            for result in results["result"]:
                for key in rawData.keys():
                    rawData[key].append(self.__getNestedParam(result,key))
            if results["cursor"] != None:
                self.__getDataFromMoralis(params, function, rawData)

        def __getCollectionByWalletAddress(self, walletAddress, outputName, outputFormat):
            # Put all parameters required from Moralis
            params = {
                "address": walletAddress, 
                "chain": "eth", 
                "limit": 100, 
                "cursor": "", 
            }
            # Put all return data desidered from "result"
            rawData = {
                "token_address": [],
                "contract_type": [],
                "name": [],
                "symbol": []
            }
            # Call main recursive function
            self.__getDataFromMoralis(params, evm_api.nft.get_wallet_nft_collections, rawData)
            # Export contents
            self.__export(rawData, outputFormat, outputName)
            return True

        def __getNFTOwnersByContract(self, smartContractAddress, outputName, outputFormat):
            # Put all parameters required from Moralis
            params = {
                "address": smartContractAddress, 
                "chain": "eth", 
                "format": "decimal", 
                "limit": 100, 
                "cursor": "", 
                "normalizeMetadata": True, 
            }
            # Put all return data desidered from "result". Nested data can be reached with special char "/"
            rawData = {
                "token_address": [],
                "token_id": [],
                "contract_type": [],
                "owner_of": [],
                "block_number": [],
                "block_number_minted": [],
                "normalized_metadata/name": [],
                "normalized_metadata/description": [],
                "normalized_metadata/image": [],
                "normalized_metadata/external_link": [],
                "normalized_metadata/animation_url": [],
                "normalized_metadata/attributes": [],
                "amount": [],
                "name": [],
                "symbol": [],
                "token_hash": [],
                "last_token_uri_sync": [],
                "last_metadata_sync": [],
            }
            # Call main recursive function
            self.__getDataFromMoralis(params, evm_api.nft.get_nft_owners, rawData)
            # Export contents into csv file
            self.__export(rawData, outputFormat, outputName)
            return True

        def __getNFTTransferByContract(self, smartContractAddress, outputName, outputFormat):
            # Put all parameters required from Moralis
            params = {
                "address": smartContractAddress, 
                "chain": "eth", 
                "format": "decimal", 
                "limit": 100, 
                "cursor": "", 
            }
            # Put all return data desidered from "result". Nested data can be reached with special char "/"
            rawData = {
                "block_number": [],
                "block_timestamp": [],
                "block_hash": [],
                "transaction_hash": [],
                "transaction_index": [],
                "log_index": [],
                "value": [],
                "contract_type": [],
                "transaction_type": [],
                "token_address": [],
                "token_id": [],
                "from_address": [],
                "to_address": [],
                "amount": [],
                "verified": [],
                "operator": []
            }
            # Call main recursive function
            self.__getDataFromMoralis(params, evm_api.nft.get_nft_contract_transfers, rawData)
            # Export contents into csv file
            self.__export(rawData, outputFormat, outputName)
            return True

        # CALLABLE FUNCTIONS

        def getCollectionByWalletAddress(self, walletAddress, outputName, outputFormat):
            self.__getCollectionByWalletAddress(walletAddress, outputName, outputFormat)

        def getNFTOwnersByContract(self, smartContractAddress, outputName, outputFormat):
            self.__getNFTOwnersByContract(smartContractAddress, outputName, outputFormat)

        def getNFTTransferByContract(self, smartContractAddress, outputName, outputFormat):
            self.__getNFTTransferByContract(smartContractAddress, outputName, outputFormat)