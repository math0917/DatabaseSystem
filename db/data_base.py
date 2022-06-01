import json
import math
import random
# make_table(table_name) : 테이블이름으로 만드는 함수
def make_table(table_name, *infos):
    data_dict = {"page_size" : 4096,"table_count" : 1 ,'table_metadata': [table_name+'_meta.json']}
    
    try:
        with open('meta_meta.json') as f:
            data = json.load(f)
        with open('meta_meta.json','w') as f:
            data["table_count"] += 1
            data['table_metadata'].append(table_name +'_meta.json')
            json.dump(data,f)
        
    except:
        with open('meta_meta.json','w') as f:
            json.dump(data_dict,f)
    info_dict = dict()
    info_dict["slotted_page_count"] = []
    info_dict["column_info"] = []
    for info in infos:
        info_dict["column_info"].append({"name" : info[0], "length" : info[1], "is_variable" : info[2], "can_null" : info[3]})
    with open(table_name+'_meta.json', 'w') as f:
        json.dump(info_dict,f)    

            
def find_table(table_name):
    with open('meta_meta.json') as f:
        data = json.load(f)
    
    if table_name + "_meta.json" in data["table_metadata"]: return True
    else: return False
    
def can_insert(slotted_page,rec):
    with open(slotted_page,"rb") as f:
        page = f.readlines()
    
    data = []
    for i in page:
        for j in i:
            data.append(j)
    if data[4]<<8 + data[5] >= len(rec):
        return True, data
    else:
        return False, data
# insert_record(table_name, record) table_name의 테이블에 record를 넣기 (slotted page 순회하며 공간에 저장하는 함수)

def byte_2(num):
    str = ''
    for i in range(16):
        if 1<<i & num:
            str = '1'+str
        else:
            str = '0'+str
    return [int(str[0:8],2), int(str[8:16],2)]

def insert_record(table_name, record):
    if find_table(table_name):
        rec = make_record(table_name, record)
        if rec:
            with open(table_name + '_meta.json') as f:
                data = json.load(f)
            flag = 0
            for i in data["slotted_page_count"]:
                this_turn_table = table_name + i+'.bin'
                bo, page = can_insert(this_turn_table,rec)
                if bo:
                    flag = 1
                    break
            # 여기에 저장할 수 있다면
            if flag:
                free_space_offset = (page[2]<<8) + page[3]
                free_space_length = (page[4]<<8) + page[5]
                data_start_idx = (page[2]<<8) + page[3] + (page[4]<<8) + page[5]
                rec_length = byte_2(len(rec))
                Entry_offset = byte_2(data_start_idx - len(rec))

                page[free_space_offset] = Entry_offset[0]
                page[free_space_offset+1] = Entry_offset[1]
                page[free_space_offset+2] = rec_length[0]
                page[free_space_offset+3] = rec_length[1]
                for i in range(len(rec)):
                    page[data_start_idx - len(rec)+i] = rec[i]
                new_free_space_offset = byte_2(free_space_offset + 4)
                new_free_space_length = byte_2(data_start_idx -len(rec) - (free_space_offset + 4))
                page[2] = new_free_space_offset[0]
                page[3] = new_free_space_offset[1]
                page[4] = new_free_space_length[0]
                page[5] = new_free_space_length[1]
                page[0] = page[0] + 1
                page[1] = page[1] + 1
                with open(this_turn_table,'wb') as f:
                    f.write(bytes(page))
            #여기에 저장할 수 없다면
            else:
                if not data["slotted_page_count"]:
                    data["slotted_page_count"].append("0")
                else:
                    data["slotted_page_count"].append(str(int(data["slotted_page_count"][-1])+1))
                length = byte_2(len(rec))
                entry_offset = byte_2(4096-len(rec))
                free_space_offset =byte_2(10)
                free_space_length = byte_2(4096-len(rec)-10)
                input_data = [1,1,free_space_offset[0],free_space_offset[1], free_space_length[0], free_space_length[1],entry_offset[0],entry_offset[1], length[0],length[1]]
                for i in range(4096-len(rec)-10):
                    input_data.append(0)
                for i in rec:
                    input_data.append(i)
                with open (table_name+data["slotted_page_count"][-1]+".bin",'wb') as f:
                    f.write(bytes(input_data))
                with open(table_name+"_meta.json", "w") as f:
                    json.dump(data,f)
        else:
            return
    else:
        print('there is no such that table_name : ', table_name)
        return
# "name": "id", "length": "8", "is_variable": "False", "can_null": "False"
# make_record(info) : 주어진 정보로 record 만드는 함수
def make_record(table_name, info):
    with open(table_name+'_meta.json') as f:
        data = json.load(f)
    null_byte = math.ceil(len(data['column_info'])/8)
    start_idx = 8 - len(data['column_info'])%8
    null_bit = [0]*8*null_byte
    data_idx = null_byte
    variable_length = 0
    for idx ,dictionary in enumerate(data['column_info']):
        if info[idx] == "null" and dictionary["can_null"] == "False":
            print(dictionary["name"],"can't be null")
            return False
        if len(info[idx]) > int(dictionary["length"]):
            print(dictionary['name'], "column length can't exceed",dictionary["length"])
            return False
        if dictionary["is_variable"] == "False" and len(info[idx]) != int(dictionary["length"]):
            print(dictionary['name'], "column is ",dictionary['length'],'fixed length')
            return False
        if info[idx] == 'null':
            null_bit[start_idx + idx] = 1
        # 고정길이면
        elif dictionary['is_variable'] == "False":
            data_idx += int(dictionary['length'])
        else:
            data_idx += 2
            variable_length+=len(info[idx])
    real_data = [0] * (data_idx + variable_length)
    
    for i in range(null_byte):
        this_turn_num = null_bit[8*i:8*i+8]
        arr = ''
        for j in this_turn_num:
            arr += str(j)
       
        real_data[i] = int(arr,2)
      
    start_idx = null_byte
    for idx, dictionary in enumerate(data['column_info']):
        if info[idx] == 'null':
            continue
        # 고정길이면
        elif dictionary['is_variable'] == "False":
            for i in info[idx]:
                real_data[start_idx] = ord(i)
                start_idx += 1
        else:
            real_data[start_idx] = data_idx
            start_idx += 1
            real_data[start_idx] = len(info[idx])
            start_idx += 1
            for i in info[idx]:
                real_data[data_idx] = ord(i)
                data_idx += 1
    
    return real_data
            
            
def select_record(table_name,id):
    print(id,'찾기')
    with open('meta_meta.json') as f:
        data = json.load(f)
    meta_file = table_name + '_meta.json'
    if meta_file in data['table_metadata']:
        with open(meta_file) as f:
            meta_data = json.load(f)
        column_length = len(meta_data['column_info'])
        null_byte = math.ceil(column_length / 8)
       
        pk_length = int(meta_data['column_info'][0]['length'])
        # 가변길이면 True, 고정길이면 False
        variable_length = meta_data['column_info'][0]['is_variable'] == "True"
        for i in meta_data['slotted_page_count']:
            file_name = table_name+i+'.bin'
            with open(file_name,'rb') as f:
                lines = f.readlines()
            data = []
            for line in lines:
                for j in line:
                    data.append(j)
            for idx in range(6,(data[2]<<8)+data[3],4):
                entry_offset = (data[idx]<<8) + data[idx+1]
                entry_length = (data[idx+2]<<8) + data[idx+3]
                # 가변길이면
                if variable_length:
                    index = null_byte 
                    data_offset = data[entry_offset + index]
                    data_length = data[entry_offset + index + 1]
                    compare = data[entry_offset + data_offset:entry_offset+data_length+data_offset]
                    string = ''

                    for i in compare:
                        string+=chr(i)
                  
                    if string == id:
                        
                        break
                # 고정길이면
                else:
                    index = null_byte
                    compare = data[entry_offset+ index:entry_offset+index+pk_length]
                    string = ''
                    for i in compare:
                        string +=chr(i)
                    
                    if string == id:
                        
                        break
            else:
                continue
            # entry_offset 발견했고 그거 이제 출력형태로 보여주면됨
            col_idx = 0
            index = entry_offset + null_byte
            finish_idx = len(meta_data['column_info'])%8
            null_bit = []
            for num in range(entry_offset, index):
                for a in reversed(range(finish_idx)):
                    
                    if data[num] & 1<<a:
                        null_bit.append(1)
                    else:
                        null_bit.append(0)
                finish_idx = 8
            
            while col_idx <column_length:
                if null_bit[col_idx]:
                    col_idx += 1
                    continue
                # 가변이라면
                if meta_data['column_info'][col_idx]['is_variable'] == "True":
                    data_idx = data[index]
                    data_length =data[index+1]
                    
                    string = ''
                    for a in data[entry_offset + data_idx:entry_offset+data_idx+data_length]:
                        string += chr(a)
                    print(meta_data['column_info'][col_idx]['name']+ " : " + string)
                    index += 2
                    col_idx += 1
                else:
                    data_length = int(meta_data['column_info'][col_idx]['length'])
                    data_idx = index
                    string = ''
                    for a in data[data_idx:data_idx +data_length]:
                        string += chr(a)
                    print(meta_data['column_info'][col_idx]['name']+ " : " + string)
                    index += data_length
                    col_idx+=1

                    
    else:
        print("There is no table_name in db")
        return 
def select_record_column(table_name, id, column_name):
    print(id,'의' +column_name+ '찾기')
    with open('meta_meta.json') as f:
        data = json.load(f)
    meta_file = table_name + '_meta.json'
    if meta_file in data['table_metadata']:
        with open(meta_file) as f:
            meta_data = json.load(f)
        column_length = len(meta_data['column_info'])
        null_byte = math.ceil(column_length / 8)
       
        pk_length = int(meta_data['column_info'][0]['length'])
        # 가변길이면 True, 고정길이면 False
        variable_length = meta_data['column_info'][0]['is_variable'] == "True"
        for i in meta_data['slotted_page_count']:
            file_name = table_name+i+'.bin'
            with open(file_name,'rb') as f:
                lines = f.readlines()
            data = []
            for line in lines:
                for j in line:
                    data.append(j)
            for idx in range(6,(data[2]<<8)+data[3],4):
                entry_offset = (data[idx]<<8) + data[idx+1]
                entry_length = (data[idx+2]<<8) + data[idx+3]
                # 가변길이면
                if variable_length:
                    index = null_byte 
                    data_offset = data[entry_offset + index]
                    data_length = data[entry_offset + index + 1]
                    compare = data[entry_offset + data_offset:entry_offset+data_length+data_offset]
                    string = ''

                    for i in compare:
                        string+=chr(i)
                  
                    if string == id:
                        
                        break
                # 고정길이면
                else:
                    index = null_byte
                    compare = data[entry_offset+ index:entry_offset+index+pk_length]
                    string = ''
                    for i in compare:
                        string +=chr(i)
                    
                    if string == id:
                        
                        break
            else:
                continue
            # entry_offset 발견했고 그거 이제 출력형태로 보여주면됨
            col_idx = 0
            index = entry_offset + null_byte
            finish_idx = len(meta_data['column_info'])%8
            null_bit = []
            for num in range(entry_offset, index):
                for a in reversed(range(finish_idx)):
                    
                    if data[num] & 1<<a:
                        null_bit.append(1)
                    else:
                        null_bit.append(0)
                finish_idx = 8
            
            while col_idx <column_length:
                if null_bit[col_idx]:
                    if meta_data['column_info'][col_idx]['name'] == column_name:
                        # 이거부터하세요 오ㅓ몰나ㅓ오라먼옮니ㅏ어리ㅏㅁㄴㅇ로마ㅣㄴ외
                        print(meta_data['column_info'][col_idx]['name']+' : '+"null")
                        return
                    col_idx += 1
                    continue
                # 가변이라면
                if meta_data['column_info'][col_idx]['is_variable'] == "True":
                    data_idx = data[index]
                    data_length =data[index+1]
                    
                    string = ''
                    for a in data[entry_offset + data_idx:entry_offset+data_idx+data_length]:
                        string += chr(a)
                    if meta_data['column_info'][col_idx]['name'] == column_name:
                        print(meta_data['column_info'][col_idx]['name']+ " : " + string)
                        return
                    index += 2
                    col_idx += 1
                else:
                    data_length = int(meta_data['column_info'][col_idx]['length'])
                    data_idx = index
                    string = ''
                    for a in data[data_idx:data_idx +data_length]:
                        string += chr(a)
                    if meta_data['column_info'][col_idx]['name'] == column_name:
                        print(meta_data['column_info'][col_idx]['name']+ " : " + string)
                        return
                    index += data_length
                    col_idx+=1

                    
    else:
        print("There is no table_name in db")
        return 
# 테이블 생성, 레코드 삽입, 레코드 검색, 컬럼 검색
# 이름, 길이, 가변인지, 널가능인지
make_table('student', ('id', '8', 'False', 'False'),('name', '10', 'True', 'False'),('depart_name', '15', 'True', 'True'),('takes', '15','true','true'))
make_table('professor', ('id', '8', 'False', 'False'),('name', '10', 'True', 'False'),('depart_name', '15', 'True', 'True'),)
rand_id = set()

for _ in range(120):
    while True:
        string =''
        for _ in range(8):
            string += str(random.randint(0,9))
        if string in rand_id:
            continue
        else:
            rand_id.add(string)
            break


for i in rand_id:
    insert_record('student',(i,"KimWonPyo", 'null'))
insert_record('professor', ('12345678', 'KangSung', 'Mathematics'))
for i in rand_id:
    select_record('student',i)

for i in rand_id:
    select_record_column('student', i ,'depart_name')