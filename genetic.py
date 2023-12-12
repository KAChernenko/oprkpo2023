import json
import copy
import sys
import csv
import requests
import os

from copy import deepcopy

import random

import time
import datetime

from time import mktime as mktime

import math

from itertools import groupby

import ast




def check_status(file,change_time):

    if os.stat(ROOT+"/statuses/"+file).st_mtime>change_time:
        change_time=os.stat(ROOT+"/statuses/"+file).st_mtime
        with open(ROOT+"/statuses/"+file, "r") as read_file:
            status = json.load(read_file)
            if status.get('stop')!=0:
                return True,change_time
    return False,change_time




def update_rebuild():
    try:
        r=requests.get('http://10.48.5.221/f_nsi_gapseries_json/',timeout=1)
        r.encoding = 'utf-8'
        text=r.text
        text = ast.literal_eval(text)
        with open("/var/www/nlmk-plansys-api/planner/genetic/rebuild.json", "w") as write_file :
        #with open("rebuild.json", "w") as write_file:
            json.dump(text,write_file,ensure_ascii=False)
    except:
        timeout=1


def update_cycles_csv():
    try:
        r=requests.get('http://10.48.5.221/f_nsi_casting_cycle_csv/',timeout=1)
        r.encoding = 'utf-8'
        text=r.text
        text=text.replace('grade_type e','grade_type')
        text=text.replace('&&','&0&')
        text=text.replace('&&','&0&')
        with open("/var/www/nlmk-plansys-api/planner/genetic/cycles.csv", "w") as f:
        #with open("cycles.csv", "w") as f:
            f.write(text)
    except:
        timeout=1

def define_order(ser,ladle):

    def combine_ids(ids_):

        del_list=[]
        for i in ids_:
            if len(i)==1:
                for j in ids_:
                    if i[0] in j and i!=j:
                        del_list.append(i)

        for i in del_list:
            ids_.pop(ids_.index(i))


        count=0
        for i in range(len(ids_)):
            for j in range(len(ids_)):
                if ids_[i]!=ids_[j] and ids_[i][-1]==ids_[j][0]:
                    ids_[i].extend(ids_[j])
                    ids_[i]=[el for el, _ in groupby(ids_[i])]
                    ids_[j]=[-1]
                    count+=1

        for i in range(count):
            ids_.pop(ids_.index([-1]))

        return ids_



    starts_fix=[]
    finishes_fix=[]
    indexes_fix=[]
    nums_fix=[]
    ids_fix=[]


    if fix_order==True:
        orders_fix=[]
        for s in ser:
            ids_fix.append(s['id'])
            indexes_fix.append(s['index'])
            orders_fix.append(int(s['order']))
            starts_fix.append(s['start'])

        x=zip(orders_fix,ids_fix,indexes_fix)
        xs=sorted(x,key=lambda tup: tup[0])
        orders_fix=[x[0] for x in xs]
        ids_fix=[x[1] for x in xs]
        indexes_fix=[x[2] for x in xs]

        '''
        if len(starts)!=0:
            if indexes_fix[0]!=1:
                return [ids_fix],True,min(starts_fix)
            else:
                return [ids_fix],False,min(starts_fix)
        else:
            return [],False,0
        '''

    starts=[]
    finishes=[]
    indexes=[]
    nums=[]
    ids=[]

    def_start=False
    start_id=-1
    for index_s,s in enumerate(ser):

        starts.append(s['start'])
        finishes.append(s['start']+(s['cycle']*s['num']+max(ladle[s['id']]))*60)
        indexes.append(s['index'])
        nums.append(s['num'])
        ids.append(s['id'])


    if len(starts)!=0:
        start_0=min(starts)
        for i in range(len(starts)):
            if starts[i]==start_0 and indexes[i]!=1:
                def_start=True
                start_id=ids[i]
                '''
                ids.pop(i)
                starts.pop(i)
                finishes.pop(i)
                indexes.pop(i)
                nums.pop(i)
                '''
                break

        x=zip(indexes,starts,finishes,ids,nums)
        xs=sorted(x,key=lambda tup: tup[0])
        indexes=[x[0] for x in xs]
        starts=[x[1] for x in xs]
        finishes=[x[2] for x in xs]
        ids=[x[3] for x in xs]
        nums=[x[4] for x in xs]

        ids_=[]

        for i in range(len(ids)):
            if indexes[i]!=1 and ids[i]!=start_id:
                ids_deltas=[]
                #ids_=[]
                id_i=[]
                for j in range(i):
                    if i!=j:
                        ids_deltas.append(abs(indexes[i]-indexes[j]-nums[j]))
                        id_i.append(ids[j])

                if ids_deltas.count(0)>1:
                    t_deltas=[]
                    id_i_=[]
                    for d in range(len(ids_deltas)):
                        if ids_deltas[d]==0:
                            #t_deltas.append(abs(finishes[i]-starts[d]))
                            t_deltas.append(abs(finishes[d]-starts[i]))
                            id_i_.append(ids[d])

                    ids_.append([id_i_[t_deltas.index(min(t_deltas))],ids[i]])

                else:
                    ids_.append([id_i[ids_deltas.index(min(ids_deltas))],ids[i]])
            else:
                ids_.append([ids[i]])


        ids_=combine_ids(ids_)


        #Выделение первой серии
        if def_start:
            for i in ids_:
                if start_id in i:
                    start_id=i
                    ids_.pop(ids_.index(i))
                    break

        if def_start:
            ids_.insert(0,start_id)



        if not fix_order:
            return ids_,def_start,start_0,False
        else:

            #Проверка порядка
            check_fix=ids_fix
            check_=[]
            for c in ids_:
                check_.extend(c)


            if check_!=check_fix:
                er=True
            else:
                er=False

            if indexes_fix[0]!=1:
                return [ids_fix],True,min(starts_fix),er
            else:
                return [ids_fix],False,min(starts_fix),er
    else:
        return [],False,0,False





def define_un_delta(un):
    with open("/var/www/nlmk-plansys-api/planner/genetic/rebuild.json", "r") as read_file:
    #with open("/Users/egornikolaev/Документы/genetic/rebuild.json", "r") as read_file:
        rebuild = json.load(read_file)

    if un==2:
        return rebuild['УН2']['rebuild'],rebuild['УН2']['gap_time']
    if un==3:
        return rebuild['УН3']['rebuild'],rebuild['УН3']['gap_time']
    if un==4:
        return rebuild['УН4']['rebuild'],rebuild['УН4']['gap_time']
    if un==6:
        return rebuild['УН6']['rebuild'],rebuild['УН6']['gap_time']

def make_ind_from_un(un):
    if un==2:
        return 0
    if un==3:
        return 1
    if un==4:
        return 2
    return 3

def join_sers(data):

    starts=[]
    for d in data:
        starts.append(d['start'])

    x=zip(starts,data)
    xs=sorted(x,key=lambda tup: tup[0])
    data=[x[1] for x in xs]

    un_sers=[[],[],[],[]]
    orders=[[],[],[],[]]
    for d in data:
        un_sers[make_ind_from_un(d['un'])].append(d)



    #----
    vps_ids=[]
    eis_ids=[]

    error_order=False

    for i in range(4):
        ids_,def_start,start_0,error_order_=define_order(un_sers[i],ladle_changes)
        if error_order_:
            error_order=True
        for j in range(len(un_sers[i])-1):
            min_delta,min_delta_gap=define_un_delta(un_sers[i][j]['un'])
            un_sers[i][j]['delta_r']=math.ceil((un_sers[i][j+1]['start']-un_sers[i][j]['start']-(un_sers[i][j]['cycle']*un_sers[i][j]['num']+max(ladle_changes[un_sers[i][j]['id']]))*60)/60)
            un_sers[i][j]['min_delta_gap']=min_delta_gap
            if un_sers[i][j]['delta_r']<0:
                un_sers[i][j]['delta_r']=1
            flag_const_delta=False

            if not fix_order:
                for k in ids_:
                    if len(k)>1 and un_sers[i][j]['id']!=k[-1] and un_sers[i][j]['id'] in k:
                        flag_const_delta=True
                        break
            else:
                for k in range(len(ids_[0])-1):
                    if define_ser(ids_[0][k+1],i,un_sers)['index']!=1 and ids_[0][k]==un_sers[i][j]['id']:
                        flag_const_delta=True

            if flag_const_delta:
                un_sers[i][j]['delta_const']=True
                un_sers[i][j]['max_delta']=un_sers[i][j]['delta_r']
                un_sers[i][j]['min_delta']=un_sers[i][j]['delta_r']
            else:
                un_sers[i][j]['delta_const']=False
                un_sers[i][j]['min_delta']=min_delta
                un_sers[i][j]['max_delta']=min_delta+30



        if un_sers[i]!=[]:
            min_delta,min_delta_gap=define_un_delta(un_sers[i][-1]['un'])
            un_sers[i][-1]['delta_r']=min_delta
            un_sers[i][-1]['min_delta_gap']=min_delta
            flag_const_delta=False
            for k in ids_:
                if len(k)>1 and un_sers[i][-1]['id']!=k[-1] and un_sers[i][-1]['id'] in k:
                    flag_const_delta=True
                    break
            if flag_const_delta:
                '''
                if un_sers[i][-1].get('delta_r')==None:
                    un_sers[i][-1]['delta_r']=30
                '''
                un_sers[i][-1]['delta_const']=True
                un_sers[i][-1]['max_delta']=un_sers[i][-1]['delta_r']
                un_sers[i][-1]['min_delta']=un_sers[i][-1]['delta_r']
            else:
                un_sers[i][-1]['delta_const']=False
                un_sers[i][-1]['min_delta']=min_delta
                un_sers[i][-1]['max_delta']=min_delta+30

        if def_start or ids_==[]:
            min_start=int(start_0)
            max_start=int(start_0)
        else:
            #Надо посчитать длину всех серий на ун
            len_un=0
            for j in range(len(un_sers[i])-1):
                len_un+=un_sers[i][j]['cycle']*un_sers[i][j]['num']*60
                len_un+=un_sers[i][j]['delta_r']*60
            if not fix_order:
                len_un+=un_sers[i][-1]['cycle']*un_sers[i][-1]['num']*60
            max_start=shift_bounds[2][1]-len_un
            min_start=int(start_0)
            if max_start<min_start:
                max_start=int(start_0)



        vps_sers=[]
        vps_ids_=[]
        if not fix_order:
            #Объединение ВПС
            for ser in un_sers[i]:
                if 'ВПС' in ser['assortment']:
                    vps_sers.append(ser)
                    vps_ids_.append(ser['id'])

            if len(vps_sers)>1:
                # если вдруг ВПС серия оказалась посередине склееных не будем ее трогать
                del_list=[]
                for vps in vps_sers:
                    for id in ids_:
                        #Вариант если окажется что нужно учитывать ВПС хвосты
                        #if vps['id'] in id and len(id)>1 and vps['id']!=id[0] and vps['id'][-1]:
                        if vps['id'] in id and len(id)>1:
                            del_list.append(vps)
                for d in del_list:
                    vps_sers.remove(d)
            if len(vps_sers)>1:
                # объединиьт все одиночные впс серии
                vps_ids_=[]
                del_list=[]
                for vps in vps_sers:
                    for id in ids_:
                        if [vps['id']]==id:
                            vps_ids_.append(id[0])
                            del_list.append(id)
                for d in del_list:
                    ids_.remove(d)
                # присоединить слева
                #если понадобится

                # присоединить справа
                #если понадобится

            if len(vps_ids_)!=0:
                if def_start:
                    ids_.insert(1,vps_ids_)
                else:
                    ids_.insert(0,vps_ids_)

            for ser in un_sers[i]:
                if ser['id'] in vps_ids_:
                    for id_ in ids_:
                        if ser['id'] in id_:
                            if len(id_)>1 and ser['id']!=id_[-1]:
                                ser['min_delta_gap']=ser['min_delta']
                                #ser['max_delta']=ser['min_delta']
                                ser['delta_r']=ser['min_delta']
                                ser['delta_const']=True
            vps_ids.extend(vps_ids_)

        eis_ids=[]
        for ser in un_sers[i]:
            if 'ЭИС' in ser['assortment'] or 'ЭАС' in ser['assortment'] :
                eis_ids.append(ser['id'])


        orders[i]=dict(order=ids_,start_0=int(start_0),def_start=def_start,min_start=min_start,max_start=max_start,eis_ids=eis_ids)




    for i in range(4):
        if un_sers[i]!=[]:
            us=un_sers[i][0]
            starts_for_to=un_sers[i][0]['start']
            plus_delta=0
            if i+12 in TO_un_ids and us['index']==1:
                for t in range(len(TO_un_ids)):
                    if i+12==TO_un_ids[t]:
                        if TO_un_starts[t]<=us['start']+plus_delta and TO_un_finishes[t]>=us['start']+plus_delta:
                            plus_delta+=TO_un_finishes[t]-us['start']-plus_delta
                            break
                        if TO_un_starts[t]<=us['start']+us['cycle']*us['num']*60+max(ladle_changes[us['id']])*60+plus_delta and TO_un_finishes[t]>=us['start']+us['cycle']*us['num']*60+max(ladle_changes[us['id']])*60+plus_delta:
                            plus_delta+=TO_un_finishes[t]-us['start']-plus_delta
                            break
                        if TO_un_starts[t]>=us['start']+plus_delta and TO_un_starts[t]<=us['start']+us['cycle']*us['num']*60+max(ladle_changes[us['id']])*60+plus_delta:
                            plus_delta+=TO_un_finishes[t]-us['start']-plus_delta
                            break
                        if TO_un_finishes[t]>=us['start']+plus_delta and TO_un_finishes[t]<=us['start']+us['cycle']*us['num']*60+max(ladle_changes[us['id']])*60+plus_delta:
                            plus_delta+=TO_un_finishes[t]-us['start']-plus_delta
                            break
            if plus_delta!=0:
                plus_delta+=1
                un_sers[i][0]['start']+=plus_delta
                orders[i]['start_0']+=plus_delta
                orders[i]['min_start']+=plus_delta
                orders[i]['max_start']+=plus_delta




    for o in orders:
        o['start_0']=int(o['start_0'])
        o['min_start']=int(o['min_start'])
        o['max_start']=int(o['max_start'])


    return un_sers,orders,vps_ids,error_order

def define_max_last_start():
    return shift_bounds[2][1]

def redefine_cyc(max_cyc,un):
    if un==0:
        return max_cyc-10
    if un ==1:
        return max_cyc-6

    return max_cyc-4

def define_cyc_(un,cyc,er):
    if un==2:
        return cyc-5,cyc+10,er
    if un==3:
        return cyc-3,cyc+6,er

    #остается только ун4,6
    return cyc-2,cyc+4,er

def ints_from_srt(s):
    l = len(s)
    integ = []
    i = 0
    while i < l:
        s_int = ''
        a = s[i]
        while '0' <= a <= '9':
            s_int += a
            i += 1
            if i < l:
                a = s[i]
            else:
                break
        i += 1
        if s_int != '':
            integ.append(int(s_int))

    return integ

def define_cyc(un,section,cyc,sulfur,carbon,sortam):
    if carbon==None:
        carbon=0
    else:
        try:
            carbon=str(carbon)
            carbon=carbon.replace(',','.')
            carbon=float(carbon)
        except:
            carbon=0
    if sulfur==None:
        sulfur=0
    else:
        try:
            sulfur=str(sulfur)
            sulfur=sulfur.replace(',','.')
            sulfur=float(sulfur)
        except:
            sulfur=0
    un_name='УН'+str(un)
    section=section.replace('х','x')        #замена кириллицы

    if len(ints_from_srt(section))==0:
        return define_cyc_(un,cyc,True)

    ints_all=ints_from_srt(section)
    width=[]
    for i in ints_all:
        if i<300:
            width.append(i)
    for i in width:
        ints_all.remove(i)

    int_section=int(sum(ints_all)/len(ints_all))


    with open("/var/www/nlmk-plansys-api/planner/genetic/cycles.csv", "r") as cycles_file:
    #with open('cycles.csv') as cycles_file:       #Дописать путь
        cycles_file_reader=csv.DictReader(cycles_file,delimiter='&')
        if un==2 or un==3:
            for row in cycles_file_reader:
                if row['unrs']==un_name and int(row['width_max'])>=int_section and int(row['width_min'])<=int_section:
                    return define_cyc_(un,int(row['casting_cycle']),False)
        else:
            list_rows=[]
            list_rows_sortam=[]
            count=0
            for row in cycles_file_reader:
                if row['unrs']==un_name and int(row['width_max'])>=int_section and int(row['width_min'])<=int_section:
                    list_rows.append(dict(min_c=float(row['min_c']),max_c=float(row['max_c']),min_s=float(row['min_s']),
                    max_s=float(row['max_s']),casting_cycle=float(row['casting_cycle']),grade_type=row['grade_type']))
                    if row['grade_type']==sortam:
                        list_rows_sortam.append(list_rows[-1])
            if len(list_rows_sortam)==0:
                for l_r in list_rows:
                    if l_r['min_c']!=l_r['max_c']:
                        if carbon>=l_r['min_c'] and carbon<=l_r['max_c']:
                            if l_r['min_s']!=l_r['max_s']:
                                if sulfur>=l_r['min_s'] and sulfur<=l_r['max_s']:
                                    return define_cyc_(un,l_r['casting_cycle'],False)
                            else:
                                return define_cyc_(un,l_r['casting_cycle'],False)
                    else:
                        if l_r['min_s']!=l_r['max_s']:
                            if carbon>=l_r['min_c'] and carbon<=l_r['max_c']:
                                return define_cyc_(un,l_r['casting_cycle'],False)

                        else:
                            break

            if len(list_rows_sortam)==1:
                return define_cyc_(un,list_rows_sortam[0]['casting_cycle'],False)
            if len(list_rows_sortam)>=1:
                for l_r in list_rows_sortam:
                    if l_r['min_c']!=l_r['max_c']:
                        if carbon>=l_r['min_c'] and carbon<=l_r['max_c']:
                            if l_r['min_s']!=l_r['max_s']:
                                if sulfur>=l_r['min_s'] and sulfur<=l_r['max_s']:
                                    return define_cyc_(un,l_r['casting_cycle'],False)
                            else:
                                return define_cyc_(un,l_r['casting_cycle'],False)
                    else:
                        if l_r['min_s']!=l_r['max_s']:
                            if carbon>=l_r['min_c'] and carbon<=l_r['max_c']:
                                return define_cyc_(un,l_r['casting_cycle'],False)
                        else:
                            break

    return define_cyc_(un,cyc,True)

def define_vyds(ser):
    if ser['min_vyd']==0 or ser['max_vyd']==0:
        with open("/var/www/nlmk-plansys-api/planner/manual_wide.json", "r") as read_file:
        #with open("/Users/egornikolaev/Документы/kc1/manual_wide.json", "r") as read_file:
            manual_wide = json.load(read_file)
            keys=list(manual_wide[ser['assortment']].keys())
            min_vyd=manual_wide[ser['assortment']][keys[0]]['min']
            max_vyd=manual_wide[ser['assortment']][keys[0]]['max']
    if ser['min_vyd']==0:
        ser['min_vyd']=min_vyd
    if ser['max_vyd']==0:
        ser['max_vyd']=max_vyd

    return ser


def add_ladle_change_list(ser,ladle_list_i,id_max,index): #обязательный аргумент index
    ladle_change_list_out=[]
    uniq=list(set(ladle_list_i))
    uniq.sort() #упорядочить индексы перековошовок
    col_changes=len(uniq)-1
    for i in range(col_changes+1):

        change_i=dict()
        change_ind=ladle_list_i.index((uniq[i]))
        change_i['id_list']=id_max
        change_i['index_list']=change_ind+index #смещение на + index
        change_i['start_list']=ser['start']+change_ind*ser['cycle']*60+uniq[i]*60

        if i==col_changes:
            change_i['num_list']=ser['num']-change_ind
        else:
            change_ind_next=ladle_list_i.index((uniq[i+1]))
            change_i['num_list']=change_ind_next-change_ind

        ladle_change_list_out.append(change_i)

        id_max+=1

    ser['ladle_change_list']=ladle_change_list_out
##  пока убрать, потому что непредсказуемо ведет себ при повторном запуске
##    if col_changes>0:
##        ser['ladle_change']=ladle_change_list_out[-1]['index_list']+ser['index']-2
##    else:
##        ser['ladle_change']=ser['ladle_change_def']


    return ser,id_max

def make_ladle_change(num,un,ladle_change_value,index):

    #если указаний по перековшовке не получено, считаем, что она по нормативу
    if ladle_change_value==None:
        ladle_change_value=0

    #пока без внеплановых перековшовок
    ladle_change_value=0

    with open("/var/www/nlmk-plansys-api/planner/ladle_manual.json", "r") as read_file:
    #with open("/Users/egornikolaev/Документы/kc1/ladle_manual.json", "r") as read_file:
        ladle_manual = json.load(read_file)
    if un==2:
        min_ladle=ladle_manual['УН2']['min']
        max_ladle=ladle_manual['УН2']['max']
    if un==3:
        min_ladle=ladle_manual['УН3']['min']
        max_ladle=ladle_manual['УН3']['max']
    if un==4:
        min_ladle=ladle_manual['УН4']['min']
        max_ladle=ladle_manual['УН4']['max']
    if un==6:
        min_ladle=ladle_manual['УН6']['min']
        max_ladle=ladle_manual['УН6']['max']

    vpo_un_min=[]
    ladle_change_list=[]

    num_plavs_for_ladle_change=num+index-1-ladle_change_value
    ladle_change_num=ladle_change_value

    num_plavs_for_ladle_change=num-ladle_change_value #переопределить num_plavs_for_ladle_change

    count=0
    count_plavs=0 #счетчик плавок до перековшовки

    while num_plavs_for_ladle_change>max_ladle:
        count_frac=2
        ladle_change_num=math.ceil(num_plavs_for_ladle_change/count_frac)
        while ladle_change_num>max_ladle:
            count_frac+=1
            ladle_change_num=math.ceil(num_plavs_for_ladle_change/count_frac)
        if ladle_change_num<min_ladle:
            ladle_change_num=min_ladle

        for i in range(ladle_change_num):
            if index==1 and i==0:
                vpo_un_min.append(True)
                ladle_change_list.append(0)
                #ladle_change_list.append(count_plavs) #счетчик плавок до перековшовки
            else:
                if i==0 and count!=0:
                    vpo_un_min.append(True)
                    ladle_change_list.append(count)
                    #ladle_change_list.append(count_plavs) #счетчик плавок до перековшовки
                else:
                    vpo_un_min.append(False)
                    ladle_change_list.append(count)
                    #ladle_change_list.append(count_plavs) #счетчик плавок до перековшовки
        count+=9
        num_plavs_for_ladle_change = num_plavs_for_ladle_change - ladle_change_num
        count_plavs=count_plavs+ladle_change_num #счетчик плавок до перековшовки

    if num_plavs_for_ladle_change!=0:
        for i in range(num-len(vpo_un_min)):
            if index==1 or count!=0:
                if i==0:
                    vpo_un_min.append(True)
                    ladle_change_list.append(count)
                    #ladle_change_list.append(count_plavs) #счетчик плавок до перековшовки
                else:
                    vpo_un_min.append(False)
                    ladle_change_list.append(count)
                    #ladle_change_list.append(count_plavs) #счетчик плавок до перековшовки
            else:
                vpo_un_min.append(False)
                ladle_change_list.append(count)
                #ladle_change_list.append(count_plavs) #счетчик плавок до перековшовки

    return vpo_un_min,ladle_change_list




def make_ladle_change_(num,un,ladle_change_value,index):

    #если указаний по перековшовке не получено, считаем, что она по нормативу
    if ladle_change_value==None:
        ladle_change_value=0

    with open("/var/www/nlmk-plansys-api/planner/ladle_manual.json", "r") as read_file:
    #with open("/Users/egornikolaev/Документы/kc1/ladle_manual.json", "r") as read_file:
        ladle_manual = json.load(read_file)
    if un==2:
        min_ladle=ladle_manual['УН2']['min']
        max_ladle=ladle_manual['УН2']['max']
    if un==3:
        min_ladle=ladle_manual['УН3']['min']
        max_ladle=ladle_manual['УН3']['max']
    if un==4:
        min_ladle=ladle_manual['УН4']['min']
        max_ladle=ladle_manual['УН4']['max']
    if un==6:
        min_ladle=ladle_manual['УН6']['min']
        max_ladle=ladle_manual['УН6']['max']

    vpo_un_min=[]
    ladle_change_list=[]


    if ladle_change_value==0:
        num_plavs_for_ladle_change=num+index-1
        count_frac=2
        ladle_change_num=math.ceil(num_plavs_for_ladle_change/count_frac)
        while ladle_change_num>max_ladle:
            count_frac+=1
            ladle_change_num=math.ceil(num_plavs_for_ladle_change/count_frac)
        if ladle_change_num<min_ladle:
            ladle_change_num=min_ladle

        count=0
        for i in range(num):
            k=i+index
            if (k-1)/ladle_change_num==(k-1)//ladle_change_num and k-1!=0:
                count+=9
            ladle_change_list.append(count)
            if (k-1)/ladle_change_num==(k-1)//ladle_change_num:
                vpo_un_min.append(True)
            else:
                vpo_un_min.append(False)
    else:
        list_of_changes=[]
        count_plavs=ladle_change_value
        num_plavs_for_ladle_change=num+index-1
        ladle_change_num=ladle_change_value
        num_plavs_for_ladle_change-=ladle_change_num
        while True:
            if num_plavs_for_ladle_change>min_ladle:
                count_frac=2
                ladle_change_num=math.ceil(num_plavs_for_ladle_change/count_frac)
                while ladle_change_num>max_ladle:
                    count_frac+=1
                    ladle_change_num=math.ceil(num_plavs_for_ladle_change/count_frac)
                num_plavs_for_ladle_change=num_plavs_for_ladle_change-ladle_change_num
                count_plavs+=ladle_change_num
                list_of_changes.append(count_plavs)
            else:
                break
        count=0
        for i in range(num):
            if index+i in list_of_changes:
                count+=9
                vpo_un_min.append(True)
            else:
                vpo_un_min.append(False)
            ladle_change_list.append(count)

    return vpo_un_min,ladle_change_list


def TO(TO):
    to_starts=[]
    to_ends=[]

    TO_un_ids=[]
    TO_un_starts=[]
    TO_un_finishes=[]

    for t in TO:
        if t['ag'] in [1,2,3]:
            to_starts.append(t['start'])
            to_ends.append(t['finish'])

        if t['ag'] in [12,13,14,15]:
            TO_un_ids.append(t['ag'])
            TO_un_starts.append(t['start'])
            TO_un_finishes.append(t['finish'])

    x=zip(TO_un_ids,TO_un_starts,TO_un_finishes)
    xs=sorted(x,key=lambda tup: tup[1])
    TO_un_ids=[x[0] for x in xs]
    TO_un_starts=[x[1] for x in xs]
    TO_un_finishes=[x[2] for x in xs]


    return to_starts,to_ends,TO_un_ids,TO_un_starts,TO_un_finishes

def miks_ids(order):

    if fix_order or len(order['order'])==1:
        return miks_ids_not(order),-1
    last_vps_ser=-1
    ids=copy.deepcopy(order['order'])
    eis_ids=order['eis_ids']

    #перемешать впс серии относительно друг друга
    if len(vps_ids)>0 and ids!=[]:
        for i in range(len(ids)):
            if i!=0 or not order['def_start']:
                flag=True
                for j in ids[i]:
                    if j not in vps_ids:
                        flag=False
                        break
                if flag:
                    random.shuffle(ids[i])
                    last_vps_ser=ids[i][-1]

    if order['def_start']:
        ids_=ids[0]
        ids.pop(0)
    else:
        ids_=[]


    #---------- Э-серии
    all_ids=[]
    eis_stacks=[]
    if eis_ids!=[]:
        for i in ids:
            if list(set(i) & set(eis_ids))!=[]:
                eis_stacks.append(i)
    if eis_stacks!=[]:
        random.shuffle(eis_stacks)
        for i in eis_stacks:
            ids_.extend(i)
            ids.remove(i)

        '''
        if order['def_start']:
            for i in eis_stacks:
                ids_.extend(i)
                ids.remove(i)
        else:
            True==True
            #рандомно добавляем стеки серий

            #считаем, кончаются они до то, или после

            #если до, и разнца небольшая, добавляем ЭИС серии (все)
        '''
    #--------------  Э-серии


    random.shuffle(ids)
    for i in ids:
        ids_.extend(i)


    return ids_,last_vps_ser

def miks_ids_not(order):
    ids=copy.deepcopy(order['order'])
    ids_=[]
    for i in ids:
        ids_.extend(i)
    return ids_


def define_ser(id,un,un_sers):
    for s in un_sers[un]:
        if s['id']==id:
            return s

def define_min_max_num(num):
    if num>10:
        return num-2,num
    else:
        return num-1,num


def def_gen(un_sers):
    ind=0
    def_gen=[[],[],[],[]]
    for i in range(4):
        for us in un_sers[i]:
            min_cyc,max_cyc,error_cyc=define_cyc(us['un'],us['section'],us['cycle'],us.get('sulfur'),us.get('carbon'),us.get('assortment'))
            assortment=us.get('assortment')
            if assortment==None:
                assortment=''
            nsi_cyc=redefine_cyc(max_cyc,i)
            if fix_cycle:
                max_cyc=nsi_cyc
                min_cyc=nsi_cyc
            min_num,max_num=define_min_max_num(us['num'])
            def_gen[i].append(dict(id=us['id'],un=us['un'],start=us['start'],min_cyc=min_cyc,max_cyc=max_cyc,min_vyd=us['min_vyd'],
            max_vyd=us['max_vyd'],num=us['num'],delta_r=us['delta_r'],delta_const=us['delta_const'],min_delta=us['min_delta'],
            min_delta_gap=us['min_delta_gap'],max_delta=us['max_delta'],index=us['index'],cyc=us['cycle'],min_num=min_num,max_num=max_num,
            assortment=assortment))

            #для фиксирования циклов
            def_gen[i][-1]['cyc']=nsi_cyc


    return def_gen,error_cyc



def def_pop(def_gen,N):
    pop=[]
    for i in range(N):
        pop.append([[],[],[],[]])

        melting_kv_=copy.deepcopy(melting_kv)
        for m in melting_kv_:
            if m['min_blow']<m['max_blow']:
                m['blow']=random.randint(m['min_blow'],m['max_blow'])


        for j in range(4):
            ids,last_vps_ser=miks_ids(orders[j])

            if orders[j]['def_start'] or orders[j]['min_start']==orders[j]['max_start']:
                start_=orders[j]['start_0']
            else:
                start_=random.randrange(orders[j]['min_start'], orders[j]['max_start'], 60)

            for s in range(len(ids)):
                us=define_ser(ids[s],j,def_gen)

                if us['delta_const']:
                    delta_r=us['delta_r']
                else:
                    delta_r=random.randrange(us['min_delta'], us['max_delta'], 1)

                pop[-1][j].append(dict(index=us['index'],id=us['id'],un=us['un'],start=start_,min_cyc=us['min_cyc'],
                max_cyc=us['max_cyc'],min_vyd=us['min_vyd'],max_vyd=us['max_vyd'],num=us['num'],delta_r_def=us['delta_r'],
                delta_const=us['delta_const'],min_delta=us['min_delta'],min_delta_gap=us['min_delta_gap'],
                max_delta=us['max_delta'],cyc=random.randint(us['min_cyc'],us['max_cyc']),delta_r=delta_r,
                min_num=us['min_num'],max_num=us['max_num'],melting_kv=melting_kv_,assortment=us['assortment']))

                if us['id'] in vps_ids:
                    pop[-1][j][-1]['delta_r']=pop[-1][j][-1]['min_delta']
                    pop[-1][j][-1]['delta_const']=True
                if us['id']==last_vps_ser:
                    pop[-1][j][-1]['delta_const']=False
                    pop[-1][j][-1]['delta_r']=random.randrange(us['min_delta'], us['max_delta'], 1)


    '''
    pop.append([[],[],[],[]])
    for j in range(4):
        ids=miks_ids_not(orders[j])
        for s in range(len(ids)):
            us=define_ser(ids[s],j,def_gen)
            pop[-1][j].append(dict(index=us['index'],id=us['id'],un=us['un'],start=us['start'],min_cyc=us['min_cyc'],
            max_cyc=us['max_cyc'],min_vyd=us['min_vyd'],max_vyd=us['max_vyd'],num=us['num'],delta_r_def=us['delta_r'],
            delta_const=us['delta_const'],min_delta=us['min_delta'],min_delta_gap=us['min_delta_gap'],
            max_delta=us['max_delta'],cyc=us['cyc'],delta_r=us['delta_r'],min_num=us['min_num'],
            max_num=us['max_num'],melting_kv=melting_kv,assortment=us['assortment']))
    '''


    return pop





def fitness_func_3(pop):
    to_lines=[]
    for i in range(len(to_starts)):
        to_lines.append({'t1':to_starts[i],'t2':to_ends[i]})


    def make_and_sum_figs(pers,graf):
        #graf_=copy.copy(graf)

        #Определдение blow
        for un in range(4):
            if pers[un]!=[]:
                delta_42=0
                for m in pers[un][0]['melting_kv']:
                    delta_42+=m['blow']+m['melts_interval']
                delta_42=int((delta_42/len(melting_kv))*60)
                delta_84=delta_42*2
                break

        for un in range(4):
            if len(pers[un])!=0:
                start_un=pers[un][0]['start']
                for index_p,p in enumerate(pers[un]):
                    start_kv=start_un-delta_42-p['max_vyd']*60
                    end_kv=start_un-p['min_vyd']*60

                    r0=(delta_42)/(end_kv-start_kv-delta_42)
                    #рассчитаем значения для ускорения
                    t_delta=int((end_kv-start_kv)/grid_val)

                    cyc_delta=p['cyc']*60
                    ladle_count=0
                    id_ser=p['id']

                    for i in range(p['num']):

                        if ladle_changes[id_ser][i]>ladle_count:
                            start_kv+=ladle_changes[id_ser][i]*60
                            #end_kv+=ladle_changes[id_ser][i]*60
                            ladle_count=ladle_changes[id_ser][i]

                        x0=int((start_kv-left_ind)/grid_val)
                        graf[x0:x0+t_delta]=[g+r0 for g in graf[x0:x0+t_delta]]

                        start_kv+=cyc_delta
                        #end_kv+=cyc_delta
                    start_un+=(p['delta_r']+p['num']*p['cyc']+max(ladle_changes[id_ser]))*60



        for to in to_lines:
            x0=(to['t1']-left_ind)//grid_val
            for i in range(int((to['t2']-to['t1'])/grid_val)):
                graf[x0+i]+=1


        if max(graf)<=1:
            return max(graf)
        else:
            return sum([g*g for g in graf if g>1])


    return [make_and_sum_figs(p,copy.copy(graf)) for p in pop]






def alive(rangs,pop,undead):
    #параментр shift_plans_rangs
    alive_pop=[]
    uniq_rangs=sorted(list(set(rangs)))
    for u in uniq_rangs:
        part_pop=[]
        part_shift_plans=[]
        for index,p in enumerate(pop):
            if rangs[index]==u:
                part_pop.append(p)
                part_shift_plans.append(shift_plans_rangs[index])
        x=zip(part_shift_plans,part_pop)
        xs=sorted(x,key=lambda tup: tup[0])
        part_pop=[x[1] for x in xs]
        alive_pop.extend(part_pop)

        if len(alive_pop)>=undead:
            count=len(alive_pop)-undead
            for i in range(count):
                alive_pop.pop(-1)
            break



    return alive_pop

def check_to(start_,finish_,un):
    start=start_
    finish=finish_
    flag_=True
    delta_start=0
    if un+12 in TO_un_ids:
        for t in range(len(TO_un_ids)):
            if un+12==TO_un_ids[t]:
                if TO_un_starts[t]<=start+delta_start and TO_un_finishes[t]>=start+delta_start:
                    flag_=False
                if TO_un_starts[t]<=finish+delta_start and TO_un_finishes[t]>=finish+delta_start:
                    flag_=False

                if TO_un_starts[t]>=start+delta_start and TO_un_starts[t]<=finish+delta_start:
                    flag_=False
                if TO_un_finishes[t]>=start+delta_start and TO_un_finishes[t]<=finish+delta_start:
                    flag_=False
                if flag_==False:
                    delta_start+=TO_un_finishes[t]-start


    if delta_start==0:
        return 0,0
    else:
        return delta_start//60+1,delta_start


def death(pop,al):
    del_list=[]
    new_pop=[]
    shift_rigth_bound_rangs=[]
    for index_i,i in enumerate(pop):
        flag_add=True
        flag_to=True
        max_starts=[]
        for un in range(4):
            if i[un]!=[]:
                starts_for_to=[]
                finishes_for_to=[]

                start_un=i[un][0]['start']
                finish_un=start_un+i[un][0]['num']*i[un][0]['cyc']*60+max(ladle_changes[i[un][0]['id']])*60

                #num_to_un=TO_un_ids.count(un+12)
                delta_start,delta_start_sec=check_to(start_un,finish_un,un)
                if delta_start!=0:
                    pop[index_i][un][0]['start']+=delta_start
                    start_un+=delta_start_sec
                    finish_un+=delta_start_sec


                for j in range(len(i[un])-1):
                    start_un+=(i[un][j]['cyc']*i[un][j]['num']+max(ladle_changes[i[un][j]['id']])+i[un][j]['delta_r'])*60
                    finish_un=start_un+i[un][j+1]['num']*i[un][j+1]['cyc']*60+max(ladle_changes[i[un][j+1]['id']])*60
                    delta_start,delta_start_sec=check_to(start_un,finish_un,un)
                    if delta_start!=0:
                        if i[un][j]['delta_const']==True or ('Э' in i[un][j]['assortment'] and 'Э' in i[un][j+1]['assortment']):
                            flag_to=False
                        else:
                            pop[index_i][un][0]['delta_r']+=delta_start
                            start_un+=delta_start_sec
                            finish_un+=delta_start_sec



                if start_un>shift_bounds[2][1]:
                    flag_add=False
                    max_starts.append(start_un)


        if flag_to:
            new_pop.append(i)
            if flag_add:
                shift_rigth_bound_rangs.append(1)
            else:
                shift_rigth_bound_rangs.append((max(max_starts)-shift_bounds[2][1])/60+1)
        else:
            del_list.append(i)


    if shift_rigth_bound_rangs.count(1)>=al:
        return new_pop,[1 for i in shift_rigth_bound_rangs]
    else:
        for i in del_list:
            pop.remove(i)
        return pop,shift_rigth_bound_rangs


def death_(pop,al):
    del_list=[]
    new_pop=[]
    shift_rigth_bound_rangs=[]
    for index_i,i in enumerate(pop):
        flag_add=True
        flag_to=True
        max_starts=[]
        for un in range(4):
            if i[un]!=[]:
                starts_for_to=[]
                finishes_for_to=[]

                start_un=i[un][0]['start']
                finish_un=start_un+i[un][0]['num']*i[un][0]['cyc']*60+max(ladle_changes[i[un][0]['id']])*60

                for j in range(len(i[un])-1):
                    starts_for_to.append(start_un)
                    finishes_for_to.append(finish_un)
                    start_un+=(i[un][j]['cyc']*i[un][j]['num']+max(ladle_changes[i[un][j]['id']])+i[un][j]['delta_r'])*60
                    finish_un=start_un+i[un][j+1]['num']*i[un][j+1]['cyc']*60+max(ladle_changes[i[un][j+1]['id']])*60

                starts_for_to.append(start_un)
                finishes_for_to.append(finish_un)


                if start_un>shift_bounds[2][1]:
                    flag_add=False
                    max_starts.append(start_un)

                if un+12 in TO_un_ids:
                    for t in range(len(TO_un_ids)):
                        if un+12==TO_un_ids[t]:
                            for tt in range(len(starts_for_to)):
                                if TO_un_starts[t]<=starts_for_to[tt] and TO_un_finishes[t]>=starts_for_to[tt]:
                                    flag_to=False
                                if TO_un_starts[t]<=finishes_for_to[tt] and TO_un_finishes[t]>=finishes_for_to[tt]:
                                    flag_to=False

                                if TO_un_starts[t]>=starts_for_to[tt] and TO_un_starts[t]<=finishes_for_to[tt]:
                                    flag_to=False
                                if TO_un_finishes[t]>=starts_for_to[tt] and TO_un_finishes[t]<=finishes_for_to[tt]:
                                    flag_to=False
                                if flag_to==False:
                                    break

        if flag_to:
            new_pop.append(i)
            if flag_add:
                shift_rigth_bound_rangs.append(1)
            else:
                shift_rigth_bound_rangs.append((max(max_starts)-shift_bounds[2][1])/60+1)
        else:
            del_list.append(i)


    if shift_rigth_bound_rangs.count(1)>=al:
        return new_pop,[1 for i in shift_rigth_bound_rangs]
    else:
        for i in del_list:
            pop.remove(i)
        return pop,shift_rigth_bound_rangs




def shift_plan_death(pop):
    del_list=[]
    shift_plans_rangs=[]
    shift_plans_all=[]
    for p in pop:
        shift_plans_i=[0,0,0]
        for un in range(4):
            if p[un]!=[]:
                start_un=p[un][0]['start']
                for index_p,s in enumerate(p[un]):
                    cyc_delta=s['cyc']*60
                    ladle_count=0
                    id_ser=s['id']
                    for i in range(s['num']):

                        if ladle_changes[id_ser][i]>ladle_count:
                            start_un+=ladle_changes[id_ser][i]*60
                            ladle_count=ladle_changes[id_ser][i]

                        #тело сравнивание с временем суток
                        if start_un>=shift_bounds[0][0] and start_un<=shift_bounds[0][1]:
                            shift_plans_i[0]+=1
                        if start_un>shift_bounds[1][0] and start_un<=shift_bounds[1][1]:
                            shift_plans_i[1]+=1
                        if start_un>shift_bounds[2][0] and start_un<=shift_bounds[2][1]:
                            shift_plans_i[2]+=1

                        start_un+=cyc_delta
                    start_un+=s['delta_r']

        delta=0
        flag_add=True
        for j in range(3):
            if shift_plans_i[j]<shift_plans[j]:
                del_list.append(p)
                flag_add=False
                break
            delta+=shift_plans_i[j]-shift_plans[j]

        if flag_add:
            shift_plans_rangs.append(-delta)
            shift_plans_all.append(shift_plans_i)

    for i in del_list:
        pop.pop(pop.index(i))
    return pop,shift_plans_rangs,shift_plans_all



def shift_plan_rangs(pop):
    del_list=[]
    shift_plans_rangs=[]
    shift_plans_all=[]
    delta_rangs=[]

    vps_delta_rangs=[]
    for p in pop:
        shift_plans_i=[0,0,0]
        delta=0
        delta_rangs.append(1)
        vps_delta_rangs.append(1)
        for un in range(4):
            if p[un]!=[]:
                start_un=p[un][0]['start']
                for index_p,s in enumerate(p[un]):

                    #прверка минмиамальной дельты
                    if s['delta_r']<s['min_delta'] and not s['delta_const']:
                        delta_rangs[-1]+=s['min_delta']-s['delta_r']

                    #-----------

                    if s['num']!=s['max_num']:
                        delta+=(s['max_num']-s['num'])/2     #/2 чтобы улучшение плана считалось лучшим решением в сравнении с изменением количества плавок

                    cyc_delta=s['cyc']*60
                    ladle_count=0
                    id_ser=s['id']


                    if s['id'] in vps_ids and datetime.datetime.fromtimestamp(start_un).time()<datetime.time(8,0):
                        vps_delta_rangs[-1]+=(datetime.datetime(1,1,1,8,0,0)-datetime.datetime.fromtimestamp(start_un)).seconds//60
                    if s['id'] in vps_ids and datetime.datetime.fromtimestamp(start_un).time()>datetime.time(17,0):
                        vps_delta_rangs[-1]+=(datetime.datetime.fromtimestamp(start_un)-datetime.datetime(1,1,1,17,0,0)).seconds//60

                    for i in range(s['num']):

                        if ladle_changes[id_ser][i]>ladle_count:
                            start_un+=ladle_changes[id_ser][i]*60
                            ladle_count=ladle_changes[id_ser][i]

                        #тело сравнивание с временем суток
                        if start_un>=shift_bounds[0][0] and start_un<=shift_bounds[0][1]:
                            shift_plans_i[0]+=1
                        if start_un>shift_bounds[1][0] and start_un<=shift_bounds[1][1]:
                            shift_plans_i[1]+=1
                        if start_un>shift_bounds[2][0] and start_un<=shift_bounds[2][1]:
                            shift_plans_i[2]+=1

                        start_un+=cyc_delta


                    if s['id'] in vps_ids and datetime.datetime.fromtimestamp(start_un).time()<datetime.time(8,0):
                        vps_delta_rangs[-1]+=(datetime.datetime(1,1,1,8,0,0)-datetime.datetime.fromtimestamp(start_un)).seconds//60
                    if s['id'] in vps_ids and datetime.datetime.fromtimestamp(start_un).time()>datetime.time(17,0):
                        vps_delta_rangs[-1]+=(datetime.datetime.fromtimestamp(start_un)-datetime.datetime(1,1,1,17,0,0)).seconds//60
                    start_un+=s['delta_r']*60

        for j in range(3):
            delta+=abs(shift_plans_i[j]-shift_plans[j])

        shift_plans_rangs.append(delta)
        shift_plans_all.append(shift_plans_i)


    return pop,shift_plans_rangs,shift_plans_all,delta_rangs,vps_delta_rangs


def new_pop(pop,m,c):

    def copy_parent_un(un_sers):
        return [{'un':p['un'],'num':p['num'],'start':p['start'],'min_cyc':p['min_cyc'],'max_cyc':p['max_cyc'],
        'min_delta':p['min_delta'],'min_delta_gap':p['min_delta_gap'],'max_delta':p['max_delta'],'min_vyd':p['min_vyd'],
        'max_vyd':p['max_vyd'],'id':p['id'],'delta_const':p['delta_const'],'index':p['index'],'cyc':p['cyc'],
        'delta_r':p['delta_r'],'min_num':p['min_num'],'max_num':p['max_num'],'melting_kv':p['melting_kv'],
        'assortment':p['assortment']} for p in un_sers]

    def crossover(parent1,parent2):
        c1=[[],[],[],[]]
        for un in range(4):
            if parent1[un]!=[]:
                if random.random()>0.65:
                    c1[un]=copy_parent_un(parent1[un])
                else:
                    c1[un]=copy_parent_un(parent2[un])

        return c1

    def mutation(pers):


        #Изменение blow
        if random.random()>0.6:
            melting_kv_=copy.deepcopy(melting_kv)
            for melt in melting_kv_:
                if melt['min_blow']<melt['max_blow']:
                    melt['blow']=random.randint(melt['min_blow'],melt['max_blow'])
            for un in range(4):
                for i in pers[un]:
                    i['melting_kv']=melting_kv_
        #изменение порядка
        if random.random()>0.8:
            new_order=[]
            while 1:
                id_un=random.randint(0,3)
                if pers[id_un]!=[]:
                    break
            order,last_vps_ser=miks_ids(orders[id_un])

            for i in order:
                us=define_ser(i,id_un,pers)
                new_order.append(us)

                if us['id'] in vps_ids:
                    new_order[-1]['delta_r']=new_order[-1]['min_delta']
                    new_order[-1]['delta_const']=True
                if us['id']==last_vps_ser:
                    new_order[-1]['delta_const']=False
                    new_order[-1]['delta_r']=random.randrange(us['min_delta'], us['max_delta'], 1)

            pers[id_un]=new_order


        #Изменения старта первой серии
        if random.random()>0.7:
            while True:
                index_un=random.randint(0,3)
                if pers[index_un]!=[]:
                    break
            if not orders[index_un]['def_start'] and orders[index_un]['min_start']!=orders[index_un]['max_start']:
                start_=random.randrange(orders[index_un]['min_start'], orders[index_un]['max_start'], 60)
                for i in range(len(pers[index_un])):
                    pers[index_un][i]['start']=start_


        #Изменение цикла и дельт фиксированное количество раз
        for i in range(m):
            while True:
                index_un=random.randint(0,3)
                if pers[index_un]!=[]:
                    break

            if index_un>1:
                index_ser=random.randint(0,len(pers[index_un])-1)
            else:
                index_ser=0
            if random.random()>0.0:
                pers[index_un][index_ser]['cyc']=random.randint(pers[index_un][index_ser]['min_cyc'], pers[index_un][index_ser]['max_cyc'])
                if not pers[index_un][index_ser]['delta_const']:
                    if random.random()>0.8:
                        new_delta_r=random.randint(pers[index_un][index_ser]['min_delta_gap'], pers[index_un][index_ser]['max_delta'])
                    else:
                        new_delta_r=random.randint(pers[index_un][index_ser]['min_delta'], pers[index_un][index_ser]['max_delta'])
                    pers[index_un][index_ser]['delta_r']=new_delta_r
            else:
                if not pers[index_un][index_ser]['delta_const']:
                    pers[index_un][index_ser]['delta_r']=random.randint(pers[index_un][index_ser]['min_delta'], pers[index_un][index_ser]['max_delta'])


        return pers


    new_pop=[]

    pairs=40*c

    for i in range(len(pop)-1):
        for j in range(c):
            new_pop.append(mutation(crossover(pop[i],pop[i+1])))


    new_pop.extend(def_pop(def_gen,300))

    #всегда оставляем лучшего в исконном виде
    new_pop.append(pop[-0])

    return new_pop



def make_input(the_best,data,id_max):
    for i in range(len(data)):

        id_ser=data[i]['id']

        for un in range(4):
            if the_best[un]!=[]:
                start_un=the_best[un][0]['start']
                for j,s in enumerate(the_best[un]):
                    if s['id']==id_ser:
                        ser=s
                        data[i]['cycle']=s['cyc']
                        data[i]['start']=start_un
                        data[i]['num']=s['num']
                        data[i]['show_row']=data[i].get('show_row')
                        un=4         #для остановки первого цикла без флагов
                        break
                    start_un+=(s['delta_r']+s['cyc']*s['num']+max(ladle_changes[s['id']]))*60

        vpo_un_min,ladle_change_list=make_ladle_change(data[i]['num'],data[i]['un'],data[i].get('ladle_change_value'),data[i]['index'])
        data[i],id_max=add_ladle_change_list(data[i],ladle_change_list,id_max,data[i]['index']) #data[i]['index']
    return data


def define_shifts():
    #определение смен
    if 'date_start' in data.keys():
        try:
            day1_timestamp=int(float(data['date_start']))+3*60*60
            day2_timestamp=day1_timestamp+86400
        except:
            day1_timestamp=int(mktime(datetime.datetime.strptime(data['date_start'].split('T')[0], '%Y-%m-%d').timetuple()))
            day1_timestamp+=int(data['date_start'].split('T')[1].split(':')[0])*60*60+int(data['date_start'].split('T')[1].split(':')[1])+3*60*60
            day2_timestamp=day1_timestamp+86400
    else:
        if 'replanning_time' in data.keys():
            #определение суток
            if datetime.datetime.fromtimestamp(data['replanning_time']).time()>datetime.time(15,30):
                day1_timestamp=data['replanning_time']
                day2_timestamp=day1_timestamp+86400
            else:
                day2_timestamp=data['replanning_time']
                day1_timestamp=day2_timestamp-86400
        else:

            starts_for_dates=[]
            for i in data['unrs']:
                starts_for_dates.append(i['start'])
            day1_timestamp=min(starts_for_dates)
            day2_timestamp=day1_timestamp+86400
            starts_for_dates=None

    day1_date=datetime.datetime.fromtimestamp(day1_timestamp).date()
    day2_date=datetime.datetime.fromtimestamp(day2_timestamp).date()
    bounds=[datetime.time(17,30),datetime.time(1,30),datetime.time(9,30),datetime.time(17,30)]
    shift3=[int(mktime(datetime.datetime.combine(day1_date,bounds[0]).timetuple())),int(mktime(datetime.datetime.combine(day2_date,bounds[1]).timetuple()))]
    shift1=[int(mktime(datetime.datetime.combine(day2_date,bounds[1]).timetuple())),int(mktime(datetime.datetime.combine(day2_date,bounds[2]).timetuple()))]
    shift2=[int(mktime(datetime.datetime.combine(day2_date,bounds[2]).timetuple())),int(mktime(datetime.datetime.combine(day2_date,bounds[3]).timetuple()))]

    return [shift3,shift1,shift2]

def define_shift_plans():
    shift_plans=[32,32,32]
    if data.get('un_shift_plan')!=None and data.get('un_shift_plan')!=[]:

        shift_plans[0]=data['un_shift_plan']['shift3']
        shift_plans[1]=data['un_shift_plan']['shift1']
        shift_plans[2]=data['un_shift_plan']['shift2']

    if shift_plans==[0,0,0]:
        shift_plans=[32,32,32]


    return shift_plans

def define_best(rangs,shift_plans_rangs,shift_plans_all,pop):
    min_rang=min(rangs)
    best_plan=1000
    best_pers=pop[rangs.index(min_rang)]
    best_plan_all=[0,0,0]
    for index,p in enumerate(pop):
        if rangs[index]==min_rang and shift_plans_rangs[index]<best_plan:
            best_pers=p
            best_plan=shift_plans_rangs[index]
            best_plan_all=shift_plans_all[index]

    for i in best_plan_all:
        i=str(i)
    return best_pers,min_rang,best_plan,best_plan_all



def make_zero_graf():
    #для использования 3й фитнес функции опредим границы поиска
    #примем в первом приближении за максимальную выдержку просто 200мин а за минимальную 30мин
    left_ind=[]
    right_ind=[]
    for un in range(4):
        if def_gen[un]!=[]:
            #start_un_min=def_gen[un][0]['start']
            start_un_min=orders[un]['min_start']
            left_ind.append(start_un_min-def_gen[un][0]['max_vyd']*60-42*60)
            #start_un_max=def_gen[un][0]['start']
            start_un_max=orders[un]['max_start']
            for p in def_gen[un]:
                vpo_un_min,ladle_change_list=make_ladle_change(p['num'],p['un'],p.get('ladle_change_value'),p['index'])
                right_ind.append(start_un_max+p['max_cyc']*60*p['num']-p['min_vyd']*60+max(ladle_change_list)*60)
                #p.pop('index')
                start_un_max+=(p['max_delta']+p['max_cyc']*p['num']+max(ladle_change_list))*60


    left_ind=min(left_ind)
    right_ind=max(right_ind)
    graf=[0 for i in range(int((right_ind-left_ind)/grid_val))]
    return graf,left_ind


def find_best_rangs(pop,shift_plans_all,shift_plans_rangs,undead):
    new_pop=[]
    new_shift_plans_rangs=[]
    new_shift_plans_all=[]
    if shift_plans_rangs.count(0)>=alive_bound:
        for index_p,p in enumerate(pop):
            if shift_plans_rangs[index_p]==0:
                new_shift_plans_rangs.append(0)
                new_pop.append(p)
                new_shift_plans_all.append(shift_plans_all[index_p])
        flag_plan_ogrs=True
        return flag_plan_ogrs,new_shift_plans_rangs,new_shift_plans_all,new_pop
    else:
        return False,[],[],[]


def combine_metrics(rangs,shift_plans_rangs,shift_rigth_bound_rangs,delta_rangs,vps_delta_rangs):
    for i in range(len(rangs)):
        rangs[i]=rangs[i]**(1.3)*((shift_plans_rangs[i]+1)**(1/4))*((shift_rigth_bound_rangs[i])**(1/6))*(delta_rangs[i]**(1/10)*(vps_delta_rangs[i]**(1/4)))

    return rangs

def new_status(rang,plan,status_file,stop,iter):

    with open(ROOT+"/statuses/"+status_file, "r") as read_file:
        status_ = json.load(read_file)
        if status_['stop']!=stop:
            stop=status_['stop']

    with open(ROOT+"/statuses/"+status_file, "w") as write_file:
        status=dict(stop=stop,rang=rang,deviation_from_plan=plan,epoch=iter)
        json.dump(status,write_file,ensure_ascii=False)

    with open(ROOT_genetic+"/statuses/"+status_file, "w") as write_file:
        status=dict(stop=stop,rang=rang,deviation_from_plan=plan,epoch=iter)
        json.dump(status,write_file,ensure_ascii=False)


#Основные параметры

children=10                      #количество потомков от каждой пары родителей

al=150                        #количество выживших в каждом поколении

N=2000                           #начальная популяция

mut=2                           #количество мутировавших генов в у каждого потомка

grid_val=3*60                #величина сетки в секундах

multi_N=20               # Во сколько величина тестовой начальной популяции больше N

alive_bound=10          #Требуемое количество подходящих представителей в тестовой популяции
#---------


update_cycles_csv()
update_rebuild()


input_data=json.loads(sys.argv[1])
data=copy.deepcopy(input_data)

if input_data.get('fix_order')==True:
    fix_order=True
    for ser in data['unrs']:
        if ser.get('order')==None:
            fix_order=False
else:
    fix_order=False

if input_data.get('fix_cycle')==False:
    fix_cycle=False
else:
    fix_cycle=True


#Все, что связано с инциализацией по статусам
ROOT_genetic = os.path.dirname(os.path.abspath(__file__))    #это если будет бэк по статусам генетического будет
ROOT='/var/www/nlmk-plansys-api/planner'
#ROOT = os.path.dirname(os.path.abspath(__file__))

if input_data.get('status_file')!=None:
    status_file=input_data.get('status_file')
else:
    if input_data.get('ip')!=None:
        status_file=input_data.get('ip')+'.json'
    else:
        status_file='def_status.json'



with open(ROOT+"/statuses/"+status_file, "w") as write_file:
    status=dict(stop=0,rang=0,deviation_from_plan=0,epoch=0)
    json.dump(status,write_file,ensure_ascii=False)

with open(ROOT_genetic+"/statuses/"+status_file, "w") as write_file:
    status=dict(stop=0,rang=0,deviation_from_plan=0,epoch=0)
    json.dump(status,write_file,ensure_ascii=False)

change_time=0
#----------------


data['unrs']=[]

data_out={}
code=600
message=''
#добавление выдежек в суточное
for ser in input_data['unrs']:
    if ser.get('show_row')!=False and ser['num']!=0:
        if ser.get('assortment')!=None and ser.get('assortment')!='':
            data['unrs'].append(copy.deepcopy(define_vyds(ser)))
        else:
            code=400
            message="Введите сортамент для всех серий."

if code==600:
    try:

        shift_bounds=define_shifts()
        shift_plans=define_shift_plans()

        to_starts,to_ends,TO_un_ids,TO_un_starts,TO_un_finishes=TO(data['to'])

        #--------------------
        starts=[]
        for d in data['unrs']:
            starts.append(d['start'])

        x=zip(starts,data['unrs'])
        xs=sorted(x,key=lambda tup: tup[0])
        data['unrs']=[x[1] for x in xs]

        ladle_changes=dict()
        for i in data['unrs']:
            vpo_un_min,ladle_change=make_ladle_change(i['num'],i['un'],i.get('ladel_change'),i['index'])
            ladle_changes[i['id']]=ladle_change



        if data['melting_kv']==None or data['melting_kv']==[]:
            melting_kv=[{"ag": 1, "blow": 37, "melts_interval": 5, "shift_interval": 5,"min_blow": 35,"max_blow": 37},
            {"ag": 2, "blow": 37, "melts_interval": 5, "shift_interval": 5,"min_blow": 35,"max_blow": 37},
            {"ag": 3, "blow": 37, "melts_interval": 5, "shift_interval": 5,"min_blow": 35,"max_blow": 37}]
        else:
            melting_kv=data['melting_kv']
            for m in melting_kv:
                if m.get("min_blow")==None or m.get("min_blow")==0:
                    m["min_blow"]=35
                else:
                    m["min_blow"]=int(m["min_blow"])
                if m.get("max_blow")==None or m.get("max_blow")==0:
                    m["max_blow"]=37
                else:
                    m["max_blow"]=int(m["max_blow"])

        delta_42=0
        for m in melting_kv:
            delta_42+=m['blow']+m['melts_interval']
        delta_42=int((delta_42/len(melting_kv))*60)
        delta_84=delta_42*2

        un_sers,orders,vps_ids,error_order=join_sers(data['unrs'])


        def_gen,error_cyc=def_gen(un_sers)

        if error_cyc:
            code=500
            message+="Не удалось определить цикл по сечению."
        if error_order:
            code=500
            message+="Проверьте порядок серий."


        graf,left_ind=make_zero_graf()


        start_time = time.time()



        pop=def_pop(def_gen,N)



        pop,shift_rigth_bound_rangs=death(pop,al)
        pop,shift_plans_rangs,shift_plans_all,delta_rangs,vps_delta_rangs=shift_plan_rangs(pop)


        count_max=0
        best_rang=100*10000
        best_plan=0

        rangs=fitness_func_3(pop)
        rangs=combine_metrics(rangs,shift_plans_rangs,shift_rigth_bound_rangs,delta_rangs,vps_delta_rangs)
        if True:
            best_pers,min_rang,max_plan,best_plan_all=define_best(rangs,shift_plans_rangs,shift_plans_all,pop)

            new_status(min_rang/3,max_plan,status_file,0,0)

            pop=alive(rangs,pop,al)

            pop=new_pop(pop,mut,children)

            pop,shift_rigth_bound_rangs=death(pop,al)

            pop,shift_plans_rangs,shift_plans_all,delta_rangs,vps_delta_rangs=shift_plan_rangs(pop)

            rangs=fitness_func_3(pop)

            rangs=combine_metrics(rangs,shift_plans_rangs,shift_rigth_bound_rangs,delta_rangs,vps_delta_rangs)

            best_pers,min_rang,max_plan,best_plan_all=define_best(rangs,shift_plans_rangs,shift_plans_all,pop)

            for i in range(300):

                pop=alive(rangs,pop,al)
                pop=new_pop(pop,mut,children)
                pop,shift_rigth_bound_rangs=death(pop,al)
                pop,shift_plans_rangs,shift_plans_all,delta_rangs,vps_delta_rangs=shift_plan_rangs(pop)
                rangs=fitness_func_3(pop)
                rangs=combine_metrics(rangs,shift_plans_rangs,shift_rigth_bound_rangs,delta_rangs,vps_delta_rangs)

                best_pers,min_rang,max_plan,best_plan_all=define_best(rangs,shift_plans_rangs,shift_plans_all,pop)
                count_max+=1

                stop,change_time=check_status(status_file,change_time)

                if stop:
                    break


                if min_rang<best_rang:
                    best_rang=min_rang
                    best_plan=max_plan
                    count_max=0

                    new_status(min_rang/3,max_plan,status_file,0,i+1)
                if min_rang==best_rang and max_plan>best_plan:
                    best_plan=max_plan
                    count_max=0

                    new_status(min_rang/3,max_plan,status_file,0,i+1)
                if count_max==25:
                    new_status(min_rang/3,max_plan,status_file,1,i+1)
                    break


            id_max=[]
            for d in data['unrs']:
                if d.get('id')!=None:
                    id_max.append(d.get('id'))
            id_max=max(id_max)+1
            id_max_2=copy.copy(id_max)

            data_out={}
            new_data=copy.deepcopy(data)
            the_best=best_pers

            new_data['unrs']=make_input(the_best,new_data['unrs'],id_max)

            for un in range(4):
                for t in the_best[un]:
                    if the_best[un]!=[]:
                        new_data['melting_kv']=the_best[un][0]['melting_kv']
                        break

            for ser in input_data['unrs']:
                flag=True
                for ser2 in new_data['unrs']:
                    if ser2['id']==ser['id']:
                        flag=False
                        break
                if flag:
                    new_data['unrs'].append(ser)


            new_data['un_calc_plan']=best_plan_all
            data_out['new_input']=new_data
        else:
            id_max=[]
            for d in data['unrs']:
                if d.get('id')!=None:
                    id_max.append(d.get('id'))
            id_max=max(id_max)+1
            id_max_2=copy.copy(id_max)
            data_out={}
            new_data=copy.deepcopy(data)
            data_out['new_input']=new_data


        data_out['input']=data
        if stop:
            data_out['new_input']=data
        id_max=id_max_2
        for d in data_out['input']['unrs']:
            vpo_un_min,ladle_change_list=make_ladle_change(d['num'],d['un'],d.get('ladle_change_value'),d['index'])
            d,id_max=add_ladle_change_list(d,ladle_change_list,id_max,d['index']) #d['index']


        #Проверка на корректность стартов
        flag_wrong_start=False
        for ser in data_out['new_input']['unrs']:
            if ser['start']<shift_bounds[0][0]-120*60:
                flag_wrong_start=True
                break
        data_out['new_input']['wrong_start']=flag_wrong_start
    except:
        code=400
        message="Неизвестная ошибка."
        data_out['input']=data
        data_out['new_input']=[]
else:
    data_out['new_input']=[]
    data_out['input']=data


log_file=data.get('task_name')
ip=data.get('ip')
if ip==None:
    ip='def_ip'
if log_file==None:
    log_file='def_name'
time_str=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
log_file+="_time:"+time_str+'.json'
data_out['name']=log_file

data_out['code']=code
data_out['message']=message



with open("/var/www/nlmk-plansys-api/planner/genetic/logs/logs.txt", "a") as write_file:
    log=dict(ip=ip,task_name=log_file,time=time_str)
    write_file.write("\n"+str(log))

with open('/var/www/nlmk-plansys-api/planner/genetic/logs/'+log_file, "w") as write_file:
    json.dump(data_out,write_file,ensure_ascii=False)
'''
with open('new_input.json', "w") as write_file:
    json.dump(data_out,write_file,ensure_ascii=False)
'''




print(log_file)
