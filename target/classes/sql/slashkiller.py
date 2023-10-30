import csv

def remove_trailing_backslashes(input_file, output_file):
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            new_row = []
            #print(row)
            for field in row:
                if(field.endswith('\\')):
                    new_row.append(field.replace("\\","#\\\\#"))
                else:
                    new_row.append(field.replace("\\","#\\\\#"))
            writer.writerow(new_row)

def find_error_data(input_file):
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        row_queue = []
        for row in reader:
            for field in row:
                if(("|||}|") in field):
                    row_queue.append(row)
                    print(row_queue)
                    break
            row_queue.append(row)
            if(len(row_queue) > 10):
                row_queue.pop(0)
input_filename = "data\danmu.csv"
output_filename = "data\danmu1.csv"
#find_error_data(input_filename)
remove_trailing_backslashes(input_filename, output_filename)


