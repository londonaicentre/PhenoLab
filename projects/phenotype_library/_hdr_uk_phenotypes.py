from phmlondon.hdruk_api import HDRUKLibraryClient

def main():

    client = HDRUKLibraryClient()

    # search_results = client.get_phenotypelist_from_search_term("diabetes")

    # print(search_results)
    #for idx, row in search_results.iterrows():
    #    print(row)

    codelist = client.get_phenotype_codelist(phenotype_id = "PH1690", version_id=3620, output_format="db", print_raw_output_to_file=True)
    print(codelist)

if __name__ == "__main__":
    main()