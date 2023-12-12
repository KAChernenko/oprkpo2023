import json
import pdb
import ast
import copy



#удаление плавок с sign_planner=0
def pop_future_plavs_from_fact(fact):
    #создание списка плавок которые нужно уждалить
    plavs_to_pop=[]
    for i in fact:
        #if i['start_on_kv']>replanning_time:
        if i['sign_planner']==0:
            plavs_to_pop.append(i)

    #обрезание факта
    for i in plavs_to_pop:
        fact.pop(fact.index(i))
    return fact


#пронумеровать все серии в плавке по возрастанию времени начала разливки

def add_ids_fact(fact):
    import datetime
    starts=[]
    starts_datetimes=[]
    ids=[]
    for i in fact:
        if i['start_series'] not in starts:
            starts.append(i['start_series'])

    #date_time_obj = datetime.datetime.strptime(starts[0], '%Y-%m-%d %H:%M:%S.%f')
    for i in starts:
        starts_datetimes.append( datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S'))

    x=zip(starts,starts_datetimes)
    xs=sorted(x,key=lambda tup: tup[1])
    starts=[x[0] for x in xs]

    for i in fact:
        if i['start_series'] in starts:
            i['series_id']=starts.index(i['start_series'])+1
        else:
            i['series_id']=0
    return fact



#наверное сначала нужно создать словарь чтобы было удобнее рабоать впредь
def make_dict(s):
    #print(s)
    s=s.replace('\n','')
    d = ast.literal_eval(s)
    return d
    '''
    s=s.replace("'",'')
    s=s.split(',')
    values=[]
    #pdb.set_trace()
    for i in s:
        #print(i)
        val=i.split(':')
        values.append(val[1])
    values[0]=values[0].replace(' ','')
    values[1]=values[1].replace(' ','')
    #values[2]=values[2].replace(' ','-')
    if 'True' in values[3]:
        values[3]=True
    else:
        values[3]=False
    pdb.set_trace()
    return values
    '''


#пересечение двух множеств     A.intersection(B)
def _intersection(A,B):
    C=[]
    for i in A:
        if i in B:
            C.append(i)

    return C


#объединение двух множеств     A.union(B)
def _union(A,B):
    C=copy.deepcopy(A)
    for i in B:
        if i not in C:
            C.append(i)

    return C

#добавить id серий в solution
#теперь просто добавить id серий в решщение
def add_ids(solution,data_in_full):
    #pdb.set_trace()

    #отсортировать задание по возростанию времени

    new_task1=copy.deepcopy(data_in_full['unrs'])
    times_for_sort=[]
    for i in data_in_full['unrs']:
        times_for_sort.append(i['start'])
    x=zip(times_for_sort,new_task1)
    xs=sorted(x,key=lambda tup: tup[0])
    data_in_full['unrs']=[x[1] for x in xs]



    col=[[],[],[],[],[]]
    ids=[[],[],[],[],[]]
    nums=[[],[],[],[],[]]
    num_series=1
    for i in data_in_full['unrs']:
        col[i['un']-5].append(i['num'])
        ids[i['un']-5].append(i['id'])
        nums[i['un']-5].append(num_series)
        #i['num_series']=num_series
        num_series+=1






    for i in solution:
        i['id']=ids[i['un']-5][0]
        i['num_series']=nums[i['un']-5][0]
        col[i['un']-5][0]-=1
        if col[i['un']-5][0]==0:
            col[i['un']-5].pop(0)
            ids[i['un']-5].pop(0)
            nums[i['un']-5].pop(0)


    return solution


#добавить номера внутри серии для каждой плавки
def add_num_in_series(solution,data_in_full):
    solution=add_ids(solution,data_in_full)



#создать множество P
def make_P(solution,T):
    #pdb.set_trace()
    t1=T[0]
    t2=T[1]
    P=[]
    for i in solution:
        if i['start_on_kv']>=t1 and i['start_on_kv']<=t2:
            #словари ломают преобразование во множества
            d=dict(num_series=i['id'],num=i['num'])
            P.append(d)
    return P

#соддать множество F
def make_F(fact,T):
    t1=T[0]
    t2=T[1]
    F=[]
    for i in fact:
        start_on_kv=define_start_on_kv_fact(i['route'])
        if start_on_kv>=t1 and start_on_kv<=t2:
            #словари ломают преобразование во множества
            d=dict(num_series=i['id_series'],num=i['num_melt_series'])
            F.append(d)
    return F

def K_time1(F1,P1,e):
    K=0
    for f in F1:
        for p in P1:
            if abs(define_start_on_kv_fact(f['route'])-p['start_on_kv'])<=e and f['id_series']==p['id'] and f['num_melt_series']==p['num']:
                K+=1

    if len(F1)!=0:
        return K/len(F1)
    else:
        return 0

def K_time2(F1,P1,e):
    K=0
    for f in F1:
        for p in P1:
            if abs(define_start_on_kv_fact(f['route'])-p['start_on_kv'])<=e and f['id_series']==p['id'] and f['num_melt_series']==p['num'] and define_num_kv_fact(f['route'])==p['kv']:
                K+=1

    if len(F1)!=0:
        return K/len(F1)
    else:
        return 0


#для определения начала по кв из факта
def define_start_on_kv_fact(route):
    for i in route:
        if i['agr_name']=='kv':
            return i['start']

def define_num_kv_fact(route):
    for i in route:
        if i['agr_name']=='kv':
            return i['agr_code']


def make_F1(FiP,fact):
    F1=[]
    for i in FiP:
        for j in fact:
            if i['num_series']==j['id_series'] and i['num']==j['num_melt_series']:
                F1.append(j)

    return F1

def make_P1(FiP,plan):
    P1=[]
    for i in FiP:
        for j in plan:
            if i['num_series']==j['id'] and i['num']==j['num']:
                P1.append(j)

    return P1


def make_ips(logs):
    #вытащить ip
    ips=[]
    for i in logs:
        if i['ip'] not in ips:
            ips.append(i['ip'])
    return ips

def check_file(file):
    with open(file, "r") as read_file:
        data = json.load(read_file)
    return data





#просто парсинг logs.txt
def watch_logs():
    logs=[]
    with open("logs.txt", "r") as read_file:
        line = read_file.readline()
        while line:
            if line!='\n':
                logs.append(make_dict(line))
            line = read_file.readline()
    return logs
    '''
    all_logs=read_file.read()
    all_logs=all_logs.split('\n')'''


with open("input.json", "r") as read_file:
    input = json.load(read_file)


#Интервал времени
#T=[0,1613687400]
T=input['T']

#Дельта для K время
#e=5*60
e=input['e']*60

#обработка плана
#with open("17.2.2021-18:58:36.json", "r") as read_file:
with open(input['plan'], "r") as read_file:
    plan = json.load(read_file)
    data_in_full=(plan['data_in_full'])
    if plan['solution_future']['recalc']==[]:
        plan=add_ids(plan['solution_future']['plan'],plan['data_in_full'])
        #plan=plan['solution_future']['plan']
    else:
        plan=add_ids(plan['solution_future']['recalc'],plan['data_in_full'])
        #plan=plan['solution_future']['recalc']
    P=make_P(plan,T)



#обработка факта
#with open("17.2.2021-17:54:37_replanning.json", "r") as read_file:
with open(input['fact'], "r") as read_file:
    fact = json.load(read_file)
    fact['fact']=pop_future_plavs_from_fact(fact['fact'])
    #fact['fact']=add_ids_fact(fact['fact'])
    F=make_F(fact['fact'],T)


FiP=_intersection(F,P)
FuP=_union(F,P)

'''
for i in fact['fact']:
    if i['num_series']==2 and i['num_melt_series']==2:
        print(i)
pdb.set_trace()
'''
F1=make_F1(FiP,fact['fact'])
P1=make_P1(FiP,plan)


#K пересечения
if len(FuP)!=0:
    K_per=len(FiP)/len(FuP)
else:
    "Деление на ноль"

#K время
K_t1=K_time1(F1,P1,e)
K_t2=K_time2(F1,P1,e)

#K сходства
K1=K_per*K_t1
K2=K_per*K_t2

print('Мощность плана', len(plan))
print('Мощность факта', len(fact['fact']))
print('Мощность FiP',len(FiP),',','Мощность FuP',len(FuP))
print('--------------')
print('K пересечения',K_per)
print('K время1',K_t1)
print('K время2',K_t2)
print('Мощность совпадений в пересечении1',K_t1*len(F1))
print('Мощность совпадений в пересечении2',K_t2*len(F1))

print('K сходства1',K1)
print('K сходства2',K2)
