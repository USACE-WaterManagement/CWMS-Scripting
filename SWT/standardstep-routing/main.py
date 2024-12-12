import os
from hecdss import HecDss, ArrayContainer

OUTPUT_DIR = "./output"
INPUT_DIR = "./input"

def test1():
     # Example working with an array
    dss = HecDss(os.path.join(OUTPUT_DIR, "my-dss-file.dss"))
    print(f" record_count = {dss.record_count()}")
    array_ints = ArrayContainer.create_float_array([1.0, 3.0, 5.0, 7.0])
    array_ints.id = "/test/float-array/redshift////"
    dss.put(array_ints)
    print(f"record_type = {dss.get_record_type(array_ints.id)}")
    read_array = dss.get(array_ints.id)
    print(read_array)
    dss.close()

def test2():
    # Open a DSS file
    dss = HecDss(os.path.join(OUTPUT_DIR, "my-dss-file.dss"))

    print('\n\n', 'items: ',[str(x) for x in dss.get_catalog().items], '\n\n')

    # Retrieve and print data
    data_path = "/test/float-array/redshift////"
    data = dss.get(data_path)
    #data.id = data_path
    print(type(data))
    print(data)

    data.values = data.values * 2
    print(data)
    print(type(data_path))
    # Save changes to DSS file
    dss.put(data_path)
    dss.close()

if __name__ == "__main__":
    test1()
    test2()