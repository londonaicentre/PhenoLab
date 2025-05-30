from phmlondon.definition import Definition

def create():
    diabetes = Definition.from_json('diabetes_mellitus_not_type1_SNOMED_7b322f7f.json')
    # print(diabetes)

    diabetes.save_to_dynamoDB()

def load():
    diabetes = Definition.load_from_dynamoDB('7b322f7f')
    print(diabetes)

if __name__ == "__main__":
    load()