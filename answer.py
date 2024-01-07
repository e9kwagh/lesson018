"""this is the new file """
import csv


def simple_interest(p=123456, t=23, i=0.08):
    """new file"""
    p += p * t * i
    final = round(p)
    newval = "{:,.0f}".format(final)
    return newval


def compound_interest(p=123456, t=23, r=0.08):
    """func"""
    amount = p * ((1 + r) ** t)
    final = round(amount)
    newval = "{:,}".format(final)
    return newval


def compound_interest_with_payments(principal, payment, term, rate, end_of_period=True):
    if end_of_period:
        for _ in range(term):
            principal *= 1 + rate
            principal += payment
        new_val = "{:,.2f}".format(principal)
        return new_val

    for _ in range(term):
        principal += payment
        principal *= 1 + rate
    new_val = "{:,.2f}".format(principal)
    return new_val


def files_innerjoin(file_path1, file_path2, **kwargs):
    """Inner join"""
    key_join = kwargs.get("key_join", [])
    output_file = kwargs.get("output_file", "results.csv")

    data_dict1 = {}
    with open(file_path1, "r", encoding="utf-8") as f1:
        reader1 = csv.DictReader(f1)
        for row in reader1:
            key = tuple(row.get(k, None) for k in key_join)
            data_dict1[key] = row

    joined_data = []
    with open(file_path2, "r", encoding="utf-8") as f2:
        reader2 = csv.DictReader(f2)
        for row in reader2:
            key = tuple(row.get(k, None) for k in key_join)
            if key in data_dict1:
                current_data = data_dict1[key].copy()
                current_data.update(row)
                joined_data.append(current_data)

    with open(output_file, "w", encoding="utf-8", newline="") as last_file:
        fieldnames = reader1.fieldnames + [
            f for f in reader2.fieldnames if f not in key_join
        ]
        writer = csv.DictWriter(last_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(joined_data)


def files_leftouterjoin(file_path1, file_path2, **kwargs):
    """Left outer"""
    key_join = kwargs.get("key_join", [])
    output_file = kwargs.get("output_file", "left_results.csv")

    data_dict1 = {}
    with open(file_path1, "r", encoding="utf-8") as f1:
        reader1 = csv.DictReader(f1)
        for row in reader1:
            key = tuple(row.get(k, None) for k in key_join)
            data_dict1[key] = row

    joined_data = []
    with open(file_path2, "r", encoding="utf-8") as f2:
        reader2 = csv.DictReader(f2)
        for row in reader2:
            key = tuple(row.get(k, None) for k in key_join)

            new_data = data_dict1[key].copy() if key in data_dict1 else {}
            new_data.update(row)
            joined_data.append(new_data)

    with open(output_file, "w", encoding="utf-8", newline="") as last_file:
        fieldnames = reader1.fieldnames + [
            f for f in reader2.fieldnames if f not in key_join
        ]
        writer = csv.DictWriter(last_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(joined_data)


def files_rightouterjoin(file_path1, file_path2, **kwargs):
    """Right outer"""
    key_join = kwargs.get("key_join", [])
    output_file = kwargs.get("output_file", "right_results.csv")

    data_dict1 = {}
    with open(file_path1, "r", encoding="utf-8") as f1:
        reader1 = csv.DictReader(f1)
        for row in reader1:
            key = tuple(row.get(k, None) for k in key_join)
            data_dict1[key] = row

    joined_data = []
    with open(file_path2, "r", encoding="utf-8") as f2:
        reader2 = csv.DictReader(f2)
        for row in reader2:
            key = tuple(row.get(k, None) for k in key_join)
            new_data = {
                k: data_dict1[key].get(k, None)
                if key in data_dict1
                else row.get(k, None)
                for k in row
            }
            joined_data.append(new_data)

    with open(output_file, "w", encoding="utf-8", newline="") as last_file:
        fieldnames = reader1.fieldnames + [
            f for f in reader2.fieldnames if f not in key_join
        ]
        writer = csv.DictWriter(last_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(joined_data)


def list_to_dict(data):
    #    output =  {key:[value]  for i in data for key,value in i.items()}
    """List to dict"""
    output = {}

    for item in data:
        for key, value in item.items():
            if key not in output:
                output[key] = [value]
            else:
                output[key].append(value)

    return output


def dict_to_list(data: dict):
    """dict to list"""
    keys = list(data.keys())
    num = len(data[keys[0]])
    output = []
    for i in range(num):
        array = {}
        for key in keys:
            array[key] = data[key][i]
        output.append(array)

    return output




def split_file(filename, split_cols):
    """Split a CSV file"""
    output_files = {}

    def clean_filename(value):
        invalid_chars = '\\/:*?"<>|'
        return "".join(char for char in value if char not in invalid_chars)

    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            split_key = tuple(row[col] for col in split_cols)

            clean_split_key = tuple(clean_filename(str(value)) for value in split_key)

            if clean_split_key in output_files:
                output_files[clean_split_key].append(row)
            else:
                output_files[clean_split_key] = [row]

    for clean_split_key, rows in output_files.items():
        output_filename = "_".join(str(value) for value in clean_split_key) + ".csv"
        output_filename = clean_filename(output_filename)
        with open(output_filename, "w", encoding="utf-8", newline="") as last_file:
            writer = csv.DictWriter(last_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    print(simple_interest())
    print(compound_interest(p=123456, t=23, r=0.08))
    print(compound_interest_with_payments(0, 100000, 35, 0.10))
    data = [{"name": "a", "age": 21}, {"name": "b", "age": 43}]
    print(list_to_dict(data))
    files_innerjoin(
        r"C:\Users\Kunal Wagh\Desktop\ie9\Day18\lesson018\newfile1.csv",
        r"C:\Users\Kunal Wagh\Desktop\ie9\Day18\lesson018\newfile2.csv",
        key_join=["id"],
    )
    files_leftouterjoin(
        r"C:\Users\Kunal Wagh\Desktop\ie9\Day18\lesson018\newfile1.csv",
        r"C:\Users\Kunal Wagh\Desktop\ie9\Day18\lesson018\newfile2.csv",
        key_join=["id"],
    )
    files_rightouterjoin(
        r"C:\Users\Kunal Wagh\Desktop\ie9\Day18\lesson018\newfile1.csv",
        r"C:\Users\Kunal Wagh\Desktop\ie9\Day18\lesson018\newfile2.csv",
        key_join=["id"],
    )
    split_file("large_file.csv", split_cols=["city", "is_married"])
