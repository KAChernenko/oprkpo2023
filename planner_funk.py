#29.9
import datetime
import math
import time
import csv
from datetime import timedelta
from pulp import *
import json
import copy

from time import mktime as mktime
from time import time as time


from itertools import *

from multiprocessing import Process, current_process


def make_noKV_pairs(series,noKV_fact,ROOT):

    def change_trip(plav,manual_wide):
        if len(plav['route'])==0:
            return '0'
        else:
            route=plav['route']
            possible_routes_maual=list(manual_wide[plav['assortment']].keys())
            possible_routes=[]
            for p in possible_routes_maual:
                possible_routes.append([])
                for vpo_i in p.split('-'):
                    possible_routes[-1].append(agr_codes_from_name(vpo_i))

            for p in range(len(possible_routes)):
                for r in range(len(plav['route'])):
                    if r==len(possible_routes[p]):
                        break
                    if route[r]['agr_code'] not in possible_routes[p][r]:
                        break
                    if route[r]['agr_code'] in possible_routes[p][r] and r==len(route)-1:
                        if r==len(possible_routes[p])-1:
                            if route[r]['agr_code'] in [7,8,10,11]:
                                return 'УНРС'
                            return 'ДОП_ВПО-УНРС'
                        trip=''
                        vpos=possible_routes_maual[p].split('-')
                        for i in range(len(possible_routes[p])-1-r):
                            if i!=len(possible_routes[p])-2-r:
                                trip='-'+vpos[-1-i]+trip
                            else:
                                if len(list(set(agr_codes_from_name(vpos[-i-1]))&set([7,8,10,11])))!=0:
                                    trip=vpos[-i-1]+trip
                                else:
                                    trip='ДОП_ВПО-'+vpos[-i-1]+trip
                        return trip
            return 'ДОП_ВПО-УНРС'



    def agr_codes_from_name(vpo_i):
        if vpo_i=='УПК(2A,2B)':
            return [11]
        if vpo_i=='УПК':
            return [10,11]
        if vpo_i=='УПК(1A,1B)':
            return [10]
        if vpo_i=='УДМ(1,2,3)':
            return [4,5,6]
        if vpo_i=='УДМ4':
            return [7]
        if vpo_i=='УДМ(4,6)':
            return [7]
        if vpo_i=='УДМ6':
            return [8]
        if vpo_i=='АЦВ':
            return [9]
        return []


    def compare_routes(assortment,route,manual_wide):
        possible_routes_maual=manual_wide[assortment].keys()
        possible_routes=[]
        for p in possible_routes_maual:
            possible_routes.append([])
            for vpo_i in p.split('-'):
                possible_routes[-1].append(agr_codes_from_name(vpo_i))


        for p in range(len(possible_routes)):
            for r in range(len(route)):
                if r==len(possible_routes[p]):
                    break
                if route[r]['agr_code'] not in possible_routes[p][r]:
                    break
                if route[r]['agr_code'] in possible_routes[p][r] and r==len(route)-1:
                    return 0

        return 1




    kvs=[]
    uns=[]
    for s in series:
        if s.get('noKV')==False and s['num']!=0:
            start_=s['start']
            for i in range(s['num']):
                uns.append(s)
                uns[-1]['arrival_on_un']=start_
                start_+=s['cycle']*60
    for f in noKV_fact:
        kvs.append(dict())
        kvs[-1]['route']=[]
        for r in f['route']:
            if r['agr_name']=='kv':
                kvs[-1]['kv']=r['agr_code']
                kvs[-1]['departure_from_kv']=r['finish']
            if r['agr_name']!='kv' and r['agr_name']!='un' and r['status']=='finish':
                kvs[-1]['route'].append(r)
    if len(kvs)==0 or len(uns)==0:
        return []

    #Создание списка возможных комбинаций ун-кв
    perms=[]
    if len(kvs)>len(uns):
        for i in itertools.permutations(kvs,len(uns)):
            l=list(i)
            ll=[]
            for j in range(len(uns)):
                ll.append({**uns[j] , **l[j]})
            perms.append(ll)
    else:
        for i in itertools.permutations(uns,len(kvs)):
            l=list(i)
            ll=[]
            for j in range(len(kvs)):
                ll.append({**kvs[j] , **l[j]})
            perms.append(ll)


    #Отбор лучшей комбинации ун-кв по выдержке
    koeffs=[]
    for i in perms:
        koeffs.append(0)
        for j in i:
            vyd=(j['arrival_on_un']-j['departure_from_kv'])//60
            if vyd<0:
                koeffs[-1]=-10000
                break
            else:
                if vyd<j['min_vyd']:
                    koeffs[-1]+=(j['min_vyd']-vyd)
                if vyd>j['max_vyd']:
                    koeffs[-1]+=(vyd-j['max_vyd'])
    best_koeff=min(koeffs)
    best_perms=[]
    for i in range(len(koeffs)):
        if koeffs[i]==best_koeff:
            best_perms.append(perms[i])


    perms=copy.deepcopy(best_perms)
    koeffs=[]

    #Отбор лучшей комбинации по маршруту
    with open(ROOT+"/ag_ids.json", "r") as read_file:
        ag_ids = json.load(read_file)

    with open(ROOT+"/manual_wide.json", "r") as read_file:
        manual_wide = json.load(read_file)

    for pp in perms:
        koeffs.append(0)
        for p in pp:
            if len(p['route'])!=0:
                koeffs[-1]+=compare_routes(p['assortment'],p['route'],manual_wide)

    best_koeff=min(koeffs)
    best_perms=perms[koeffs.index(best_koeff)]

    #Добавление дополнительных ключей в пары
    for bp in best_perms:

        #Сортировка route по времени для удобства
        if len(bp['route'])>1:
            times_for_sort=[]
            for r in bp['route']:
                times_for_sort.append(r['finish'])
            x=zip(times_for_sort,bp['route'])
            xs=sorted(x,key=lambda tup: tup[0])
            bp['route']=[x[1] for x in xs]

        bp['trip1']=change_trip(bp,manual_wide)

        #Добавление названия агрегата в route
        for r in bp['route']:
            for a in ag_ids.keys():
                if ag_ids[a]==r['agr_code']:
                    route=a
                    break
            r['agr_name']=a


    return best_perms



def rm_garbage(ROOT):
    if ROOT=='/var/www/nlmk-plansys-api/planner':
        flag=True
        for root, dirs, files in os.walk('/tmp'):
            if root!='/tmp':
                flag=False
                break
            for f in files:
                if '-pulp.mps' in f or '-pulp.sol' in f:
                    os.remove('/tmp/'+f)
            if flag==False:
                break

##def update_files(ROOT):
##
##    def update_ladle_manual(ROOT):
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_gapseries_json/',timeout=1)
##            text=r.text
##            text = ast.literal_eval(text)
##            with open(ROOT+"/ladle_manual.json", "w") as write_file:
##                json.dump(text,write_file,ensure_ascii=False)
##        except:
##            timeout=1
##
##    def update_ag_ids(ROOT):
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_ag_ids_json/',timeout=1)
##            text=r.text
##            text = ast.literal_eval(text)
##            with open(ROOT+"/ag_ids.json", "w") as write_file:
##                json.dump(text,write_file,ensure_ascii=False)
##        except:
##            timeout=1
##
##    def update_bins(ROOT):
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_bins_spend_csv/',timeout=1)
##            r.encoding = 'utf-8'
##            text=r.text
##            text="\""+text+"\""
##            text=text.replace('&','\",\"')
##            text=text.replace('\n','\"\n\"')
##            with open(ROOT+'/bins_0.csv','w') as f:
##                f.write(text)
##        except:
##            timeout=1
##    def update_durations(ROOT):
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_duration_csv/',timeout=1)
##            r.encoding = 'utf-8'
##            text=r.text
##            text="\""+text+"\""
##            text=text.replace(';','\",\"')
##            text=text.replace('\n','\"\n\"')
##            text=text.replace('\t','')
##            with open(ROOT+'/duration_between_units.csv','w') as f:
##                f.write(text)
##        except:
##            timeout=1
##
##
##
##    def update_manual_wide(ROOT):
##
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_treatment_json/',timeout=1)
##            r.encoding = 'utf-8'
##            text=r.text
##            text = ast.literal_eval(text)
##            with open(ROOT+"/manual_wide.json", "w") as write_file:
##                json.dump(text,write_file,ensure_ascii=False)
##        except:
##            timeout=1
##
##
##
##    def update_triples(ROOT):
##
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_bins_triples_csv/',timeout=1)
##            r.encoding = 'utf-8'
##            text=r.text
##            text="\""+text+"\""
##            text=text.replace('&','\",\"')
##            text=text.replace('\n','\"\n\"')
##            text=text.replace('\t','')
##            with open(ROOT+'/triples.csv','w') as f:
##                f.write(text)
##        except:
##            timeout=1
##
##    def update_bins_1(ROOT):
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_resources_json/',timeout=1)
##            r.encoding = 'utf-8'
##            text=r.text
##            text = ast.literal_eval(text)
##            with open(ROOT+"/bins_1.json", "w") as write_file:
##                json.dump(text,write_file,ensure_ascii=False)
##        except:
##            timeout=1
##
##
##    def update_sortams_trips_coeffs(ROOT):
##        try:
##            r=requests.get('http://10.48.5.221/f_nsi_coeff_json/',timeout=1)
##            r.encoding = 'utf-8'
##            text=r.text
##            text = ast.literal_eval(text)
##            with open(ROOT+"/sortams-trips-coeffs.json", "w") as write_file:
##                json.dump(text,write_file,ensure_ascii=False)
##        except:
##            timeout=1



##    import ast
##    import requests
##    import json
##    import threading
##
##    #t1 = threading.Thread(target=update_bins,args=(ROOT,))
##    t2 = threading.Thread(target=update_durations,args=(ROOT,))
##    t3 = threading.Thread(target=update_manual_wide,args=(ROOT,))
##    #t4 = threading.Thread(target=update_triples,args=(ROOT,))
##    t5 = threading.Thread(target=update_bins_1,args=(ROOT,))
##    #t6 = threading.Thread(target=update_sortams_trips_coeffs,args=(ROOT,))
##    t7 = threading.Thread(target=update_ag_ids,args=(ROOT,))
##    t8 = threading.Thread(target=update_ladle_manual,args=(ROOT,))
##
##    #t1.start()
##    t2.start()
##    t3.start()
##    #t4.start()
##    t5.start()
##    #t6.start()
##    t7.start()
##    t8.start()
##
##
##    #t1.join()
##    t2.join()
##    t3.join()
##    #t4.join()
##    t5.join()
##    #t6.join()
##    t7.join()
##    t8.join()




def planner_2(ROOT,data_out,noKV_pairs):
    def max_vpos(out_sols,TO_name,TO_start,TO_finish):
        step=5*60
        delta_bet_vpo=10*60


        def find_sols_max_vpo():
            Per_num1=len(pers)
            x=[]
            for i in range(Per_num1):
                x.append(pulp.LpVariable("x"+str(i), lowBound=0,cat='Integer'))
            problem = pulp.LpProblem('0',LpMaximize)

            f=None
            for i in range(Per_num1):
                f+=x[i]*pers[i]['weight']
            problem += f, "Функция цели"

            Count=1

            for i in to_ogrs:
                problem += x[i] == 0, str(Count)
                Count+=1

            for i in range(len(garant_ogrs)):
                f=None
                for j in range(len(garant_ogrs[i])):
                    f+=x[garant_ogrs[i][j]]
                if f!=None:
                    problem += f == 2, str(Count)
                    Count+=1

            for i in range(len(kol_pers_ogrs)):
                f=None
                for j in range(len(kol_pers_ogrs[i])):
                    f+=x[kol_pers_ogrs[i][j]]
                if f!=None:
                    problem += f == 1, str(Count)
                    Count+=1

            f=None
            for i in range(len(un_kv_ogrs)):
                f+=x[un_kv_ogrs[i]]
            if f!=None:
                problem += f == 0, str(Count)
                Count+=1


            for i in range(len(agr_performance_ogr)):
                f=None
                for j in range(len(agr_performance_ogr[i])):
                    f+=x[agr_performance_ogr[i][j]]
                if f!=None:
                    problem += f <= 1, str(Count)
                    Count+=1

            for i in range(len(max_vpo_ogrs)):
                f=None
                for j in range(len(max_vpo_ogrs[i])):
                    f+=x[max_vpo_ogrs[i][j]]
                if f!=None:
                    problem += f <= 1, str(Count)
                    Count+=1

            for i in range(len(route_ogrs)):
                f=None
                for j in range(len(route_ogrs[i])):
                    f+=x[route_ogrs[i][j]]
                if f!=None:
                    problem += f <= 1, str(Count)
                    Count+=1

            problem.solve(pulp.PULP_CBC_CMD(msg=False))

            sols=[]
            for variable in problem.variables():
                if variable.varValue==1:
                    sols.append(int(variable.name[1:]))

            return problem.status,sols


        def un_kv_ogrs():
            un_kv_ogrs=[]
            for i in range(len(pers)):
                for j in out_sols:
                    if pers[i]['num_per']==j['num_per']:
                        if pers[i]['agr']==j['route'][0]['agr']:
                            if pers[i]['arr']<j['departure_from_kv']+j['times_in_roude'][0]:
                                un_kv_ogrs.append(i)
                        if pers[i]['agr']==j['route'][-1]['agr']:
                            if pers[i]['dep']>j['arrival_on_un']-j['times_in_roude'][-1]:
                                un_kv_ogrs.append(i)

                        break

            return un_kv_ogrs


        def gen_pers():
            pers=[]
            kol_pers_ogrs=[]
            garant_ogrs=[]
            route_ogrs=[]

            for i in out_sols:

                if i['vpo_delta_cons'] and i['vpo_to_cons']:

                    pers_inds_for_route_ogrs=[]
                    for j in range(len(i['route'])):
                        kol_pers_ogrs.append([])
                        kol_pers_ogrs.append([])
                        garant_ogrs.append([])
                        pers_inds_for_route_ogrs.append([])
                        for s in range((i['route'][j]['max_vpo']*60-(i['route'][j]['dep']-i['route'][j]['arr']))//step+1):

                            if i['flag_min_un_route']:
                                w=10
                            else:
                                w=1

                            pers.append(dict(agr=i['route'][j]['agr'],dep=i['route'][j]['dep']+step*s,arr=i['route'][j]['arr'],delta=s*step,num_per=i['num_per'],max_vpo=i['route'][j]['max_vpo'],weight=w*s*step))

                            pers_inds_for_route_ogrs[-1].append(len(pers)-1)
                            kol_pers_ogrs[-1].append(len(pers)-1)
                            kol_pers_ogrs[-2].append(len(pers))
                            garant_ogrs[-1].append(len(pers)-1)
                            garant_ogrs[-1].append(len(pers))

                            pers.append(dict(agr=i['route'][j]['agr'],arr=i['route'][j]['arr']-step*s,dep=i['route'][j]['dep'],delta=step*s,num_per=i['num_per'],max_vpo=i['route'][j]['max_vpo'],weight=w*s*step))

                            if j!=0:
                                part_ogr=[len(pers)-1]
                                for n in pers_inds_for_route_ogrs[j-1]:
                                    if pers[-1]['arr']-pers[n]['dep']<i['times_in_roude'][j]:
                                        part_ogr.append(n)
                                    if len(part_ogr)>1:
                                        route_ogrs.append(part_ogr)


            return pers,kol_pers_ogrs,garant_ogrs,route_ogrs


        def agr_performance_ogr():
            agr_performance_ogr=[]
            max_vpo_ogrs=[]

            for i in range(len(pers)):
                for j in range(len(pers)):
                    part_ogr=[i]
                    part_ogr_2=[i]
                    if pers[i]['agr']==pers[j]['agr'] and i!=j and pers[i]['num_per']!=pers[j]['num_per']:

                        '''
                        if pers[i]['arr']-pers[j]['dep']<delta_bet_vpo and pers[i]['arr']-pers[j]['dep']>=0:
                            if j not in part_ogr:
                                part_ogr.append(j)

                        if pers[i]['arr']>pers[j]['arr'] and pers[i]['arr']<pers[j]['dep']:
                            if j not in part_ogr:
                                part_ogr.append(j)

                        if pers[i]['dep']>pers[j]['arr'] and pers[i]['dep']<pers[j]['dep']:
                            if j not in part_ogr:
                                part_ogr.append(j)
                        '''
                        if pers[i]['arr']-delta_bet_vpo<pers[j]['dep'] and pers[i]['arr']-delta_bet_vpo>pers[j]['arr']:
                            if j not in part_ogr:
                                part_ogr.append(j)
                        if pers[i]['dep']+delta_bet_vpo<pers[j]['dep'] and pers[i]['dep']+delta_bet_vpo>pers[j]['arr']:
                            if j not in part_ogr:
                                part_ogr.append(j)



                    if pers[i]['agr']==pers[j]['agr'] and i!=j and pers[i]['num_per']==pers[j]['num_per']:
                        if max(pers[i]['dep'],pers[j]['dep'])-min(pers[i]['arr'],pers[j]['arr'])>pers[j]['max_vpo']*60:
                            if j not in part_ogr:
                                part_ogr_2.append(j)

                    if len(part_ogr)>1 and part_ogr not in agr_performance_ogr:
                        agr_performance_ogr.append(part_ogr)

                    if len(part_ogr_2)>1 and part_ogr not in max_vpo_ogrs:
                        max_vpo_ogrs.append(part_ogr_2)


            return agr_performance_ogr,max_vpo_ogrs


        def change_out_sols():
            for i in sols:
                for j in out_sols:
                    if j['num_per']==pers[i]['num_per']:
                        for k in j['route']:
                            if k['agr']==pers[i]['agr']:
                                k['arr']=min(pers[i]['arr'],k['arr'])
                                k['dep']=max(pers[i]['dep'],k['dep'])
                                break
                        break
            return out_sols

        def max_vpo_to_ogrs():
            if len(TO_name)==0:
                return []
            to_ogrs=[]
            for p in range(len(pers)):
                if pers[p]['agr'] in TO_name:
                    for i in range(len(TO_name)):
                        if TO_name[i]==pers[p]['agr']:
                            if TO_start[i]>pers[p]['arr'] and TO_start[i]<pers[p]['dep']:
                                to_ogrs.append(p)
                                break
                            if TO_finish[i]>pers[p]['arr'] and TO_finish[i]<pers[p]['dep']:
                                to_ogrs.append(p)
                                break

            #Удалить повторы
            to_ogrs=list(set(to_ogrs))
            return to_ogrs


        def max_bad_sols():
            for s in sols:
                if not s['vpo_delta_cons']:
                    for index_r,r in enumerate(s['route']):
                        r_nearest=[]
                        l_nearest=[]
                        for ss in sols:
                            if ss!=s:
                                for rr in ss['route']:
                                    if r['agr']==rr['agr']:
                                        r_nearest.append(r['dep']-rr['arr'])
                                        l_nearest.append(r['arr']-rr['dep'])
                        r_nearest=[rn for rn in r_nearest if rn>=-10*60]
                        l_nearest=[ln for ln in l_nearest if ln>=-10*60]

                        if r_nearest==[]:
                            r_nearest=200*60
                        else:
                            r_nearest=min(r_nearest)-10*60
                            if r_nearest<=0:
                                r_nearest=0
                        if l_nearest==[]:
                            l_nearest=200*60
                        else:
                            l_nearest=min(l_nearest)-10*60
                            if l_nearest<=0:
                                l_nearest=0

                        max_vpo_deta=r['max_vpo']*60-(r['dep']-r['arr'])

                        if index_r==0:
                            l_route_delta=r['arr']-(s['departure_from_kv']+s['times_in_roude'][0])
                        else:
                            l_route_delta=r['arr']-(s['route'][index_r-1]['dep']+s['times_in_roude'][index_r-1])

                        if index_r==len(s['route'])-1:
                            r_route_delta=s['arrival_on_un']-(r['dep']+s['times_in_roude'][-1])
                        else:
                            r_route_delta=s['route'][index_r+1]['arr']-s['times_in_roude'][index_r]-r['dep']

                        r_to=[]
                        l_to=[]
                        for index_to,to in enumerate(TO_name):
                            if r['agr']==to:
                                r_to.append(r['dep']-TO_start[index_to])
                                l_to.append(r['arr']-TO_finish[index_to])
                        r_to=[rt for rt in r_to if rt>0]
                        l_to=[lt for lt in l_to if lt>0]

                        if l_to==[]:
                            l_to=200*60
                        else:
                            l_to=min(l_to)
                        if r_to==[]:
                            r_to=200*60
                        else:
                            r_to=min(r_to)



                        r['arr']-=min(l_route_delta,l_nearest,max_vpo_deta,l_to)
                        r['dep']+=min(r_route_delta,r_nearest,max_vpo_deta,r_to)


                        if r['dep']-r['arr']>r['max_vpo']*60:
                            minus_delta=r['max_vpo']-(r['dep']-r['arr'])
                            if min(l_route_delta,l_nearest,max_vpo_deta,l_to)!=0:
                                r['arr']+=minus_delta
                            else:
                                r['dep']-=minus_delta
            return sols


        def max_left_vpo():
            for s in sols:
                for index_r,r in enumerate(s['route']):
                    l_nearest=[]
                    if (r['dep']-r['arr'])//60!=r['max_vpo']:
                        for ss in sols:
                            if ss!=s:
                                for rr in ss['route']:
                                    if r['agr']==rr['agr']:
                                        l_nearest.append(r['arr']-rr['dep'])
                        l_nearest=[ln for ln in l_nearest if ln>=-10*60]
                        if l_nearest==[]:
                            l_nearest=200*60
                        else:
                            l_nearest=min(l_nearest)-10*60
                            if l_nearest<=0:
                                l_nearest=0

                        max_vpo_deta=r['max_vpo']*60-(r['dep']-r['arr'])

                        if index_r==0:
                            l_route_delta=r['arr']-(s['departure_from_kv']+s['times_in_roude'][0])
                        else:
                            l_route_delta=r['arr']-(s['route'][index_r-1]['dep']+s['times_in_roude'][index_r-1])

                        l_to=[]
                        for index_to,to in enumerate(TO_name):
                            if r['agr']==to:
                                l_to.append(r['arr']-TO_finish[index_to])
                        l_to=[lt for lt in l_to if lt>0]

                        if l_to==[]:
                            l_to=200*60
                        else:
                            l_to=min(l_to)

                        r['arr']-=min(l_route_delta,l_nearest,max_vpo_deta,l_to)
            return sols





        pers,kol_pers_ogrs,garant_ogrs,route_ogrs=gen_pers()

        un_kv_ogrs=un_kv_ogrs()
        agr_performance_ogr,max_vpo_ogrs=agr_performance_ogr()
        to_ogrs=max_vpo_to_ogrs()


        status,sols=find_sols_max_vpo()

        for i in range(fake_to):
            TO_name.pop(-1)
            TO_start.pop(-1)
            TO_finish.pop(-1)

        sols=change_out_sols()

        sols=max_left_vpo()

        if fake_to==0:
            return sols


        return max_bad_sols()



    def find_sols_2(pers,Ogrs,Ogrs2,TO_ogrs,acv_ogrs):
        Per_num1=len(pers)
        x=[]
        for i in range(Per_num1):
            x.append(pulp.LpVariable("x"+str(i), lowBound=0,cat='Integer'))

        #Переменные для включения/выключения ограничений
        y_delta=[]
        y_garant=[]
        for i in range(len(Ogrs)):
            y_garant.append(pulp.LpVariable("y_garant_"+str(i), lowBound=0,upBound=1,cat='Integer'))
        for i in range(len(Ogrs2)):
            y_delta.append(pulp.LpVariable("y_delta_"+str(i), lowBound=0,cat='Integer'))
        y_to=(pulp.LpVariable("y_to", lowBound=0,cat='Integer'))


        problem = pulp.LpProblem('0',LpMinimize)

        f=None
        for i in range(Per_num1):
            f+=x[i]*pers[i]['weight']

        for i in range(len(y_delta)):
            f+=y_delta[i]*500
        for i in range(len(y_garant)):
            f+=y_garant[i]*40000
        f+=y_to*8000

        problem += f, "Функция цели"

        Count=1
        for i in range(len(Ogrs)):
            f=None
            for j in range(len(Ogrs[i])):
                f+=x[Ogrs[i][j]]
            if f!=None:
                problem += f == 1-y_garant[i], str(Count)
                Count+=1


        for i in range(len(Ogrs2)):
            f=None
            for j in range(len(Ogrs2[i])):
                f+=x[Ogrs2[i][j]]
            if f!=None:
                target_per=Ogrs2[i][0]
                len_cons=len(Ogrs2[i])
                problem += f <= 1+y_delta[i], str(Count)
                #problem += f <= 1+y_delta[i] + (1-x[target_per])*len_cons*2, str(Count)
                Count+=1

        f=None
        for i in TO_ogrs:
            f+=x[i]
        if f!=None:
            problem += f == 0+y_to
            Count+=1
        '''
        for i in TO_ogrs:
            problem += x[i] == 0, str(Count)
            Count+=1
        '''


        for i in acv_ogrs:
            problem += x[i] == 0, str(Count)
            Count+=1



        problem.solve(pulp.PULP_CBC_CMD(msg=False))

        y_delta_off=[]

        sols=[]
        for variable in problem.variables():
            if variable.varValue==1 and 'x' in variable.name:
                sols.append(int(variable.name[1:]))
            if variable.varValue!=0 and 'y_delta_' in variable.name:
                y_delta_off.append(int(variable.name[8:]))



############ + transportation
        vpo_message=dict(delta=True,route=True,to=True,transportation=False)
        if y_to.varValue!=0:
            vpo_message['to']=False
        for i in y_garant:
            if i.varValue!=0:
                vpo_message['route']=False
                break
        for i in y_delta:
            if i.varValue!=0:
                vpo_message['delta']=False
                break


        #Если для какой то плавки нет переменных
        if [] in Ogrs:
            vpo_message['route']=False

        return problem.status,sols,vpo_message,y_delta_off

    def make_one_index(un):
        un-=2
        if un==4:
            un-=1
        return un

    def find_routes_noKV(coeffs,manual_wide,steel,dep,arr,un,kv,pair):
        all_routes=copy.deepcopy(manual_wide[steel])

        weight_coeffs=copy.deepcopy(coeffs[steel])

        '''
        if steel=='НУ (08Ю и аналоги)' or steel=='УС (C max ≥ 0,06%; S max ≥ 0,016%; Si max ≤ 0,04%)' or steel=='УС (C max ≥ 0,06%; S max ≥ 0,016%; Si max > 0,04%)':
            if un==2 or (arr-dep)/60>70:
                all_routes.pop('УДМ(1,2,3)-УДМ(4,6)')
                weight_coeffs.pop('УДМ(1,2,3)-УДМ(4,6)')

        if steel=='ЭАС без дегазации':

            if un==2:
                all_routes.pop('УДМ(1,2,3)-УДМ(4,6)')
                weight_coeffs.pop('УДМ(1,2,3)-УДМ(4,6)')
        '''


        just_routes=[]
        just_coeffs=[]
        min_max=[]
        for i in all_routes.keys():
            just_routes.append(i)
        for i in just_routes:
            just_coeffs.append(weight_coeffs[i])
        just_routes_2=copy.deepcopy(just_routes)
        for i in range(len(just_routes)):
            just_routes[i]=just_routes[i].split('-')

        if kv==1:
            for jr in just_routes:
                if jr[0]=='УДМ(1,2,3)':
                    jr.pop(0)

        if pair['trip1']!='0' and 'УНРС' not in pair['trip1']:
            if 'ДОП_ВПО' in pair['trip1']:
                part_of_trip=pair['trip1'].replace('ДОП_ВПО-','')
            else:
                part_of_trip=pair['trip1']
            for i in all_routes.keys():
                if part_of_trip in i:
                    just_routes_2=[i]
                    just_routes=[pair['trip1'].split('-')]
                    just_coeffs=[1]
                    break
        if '-УНРС' in pair['trip1']:
            just_routes_2=[]
            just_routes=[['ДОП_ВПО']]
            just_coeffs=[1]
        if pair['trip1']=='0':
            for i in range(len(just_routes)):
                just_routes[i].insert(0,'ДОП_ВПО')



        for i in range(len(just_routes)):
            min_max.append([])
            for j in range(len(just_routes[i])):
                min_max[-1].append([])
                if just_routes[i][j]=='ДОП_ВПО':
                    min=20
                    max=20
                    just_routes[i][j]=['УДМ4','УДМ6','УПК1','УПК2']
                    for t in range(4):
                        min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УДМ(1,2,3)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УДМ1','УДМ2','УДМ3']
                    for t in range(3):
                        min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УПК(2A,2B)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УПК2']
                    min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УПК(1A,1B)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УПК1']
                    min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УДМ(4,6)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УДМ4','УДМ6']
                    for t in range(2):
                        min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УПК':

                    if all_routes[just_routes_2[i]].get(just_routes[i][j])!=None:
                        min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                        max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                        just_routes[i][j]=['УПК1','УПК2']
                        for t in range(2):
                            min_max[-1][-1].append([min,max])
                    else:
                        min=all_routes[just_routes_2[i]]['УПК(1A,1B)']['min']
                        max=all_routes[just_routes_2[i]]['УПК(1A,1B)']['max']
                        min_max[-1][-1].append([min,max])

                        min=all_routes[just_routes_2[i]]['УПК(2A,2B)']['min']
                        max=all_routes[just_routes_2[i]]['УПК(2A,2B)']['max']
                        min_max[-1][-1].append([min,max])

                        just_routes[i][j]=['УПК1','УПК2']

                if type(just_routes[i][j])!=list:
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=[just_routes[i][j]]
                    min_max[-1][-1].append([min,max])
        return just_routes,just_coeffs,min_max

    def find_routes(coeffs,manual_wide,steel,dep,arr,un,kv):
        all_routes=copy.deepcopy(manual_wide[steel])

        weight_coeffs=copy.deepcopy(coeffs[steel])

        '''
        if steel=='НУ (08Ю и аналоги)' or steel=='УС (C max ≥ 0,06%; S max ≥ 0,016%; Si max ≤ 0,04%)' or steel=='УС (C max ≥ 0,06%; S max ≥ 0,016%; Si max > 0,04%)':
            if un==2 or (arr-dep)/60>70:
                all_routes.pop('УДМ(1,2,3)-УДМ(4,6)')
                weight_coeffs.pop('УДМ(1,2,3)-УДМ(4,6)')

        if steel=='ЭАС без дегазации':

            if un==2:
                all_routes.pop('УДМ(1,2,3)-УДМ(4,6)')
                weight_coeffs.pop('УДМ(1,2,3)-УДМ(4,6)')
        '''


        just_routes=[]
        just_coeffs=[]
        min_max=[]
        for i in all_routes.keys():
            just_routes.append(i)
        for i in just_routes:
            just_coeffs.append(weight_coeffs[i])
        just_routes_2=copy.deepcopy(just_routes)
        for i in range(len(just_routes)):
            just_routes[i]=just_routes[i].split('-')

        if kv==1:
            for jr in just_routes:
                if jr[0]=='УДМ(1,2,3)':
                    jr.pop(0)


        for i in range(len(just_routes)):
            min_max.append([])
            for j in range(len(just_routes[i])):
                min_max[-1].append([])
                if just_routes[i][j]=='УДМ(1,2,3)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УДМ1','УДМ2','УДМ3']
                    for t in range(3):
                        min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УПК(2A,2B)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УПК2']
                    min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УПК(1A,1B)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УПК1']
                    min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УДМ(4,6)':
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=['УДМ4','УДМ6']
                    for t in range(2):
                        min_max[-1][-1].append([min,max])
                if just_routes[i][j]=='УПК':

                    if all_routes[just_routes_2[i]].get(just_routes[i][j])!=None:
                        min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                        max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                        just_routes[i][j]=['УПК1','УПК2']
                        for t in range(2):
                            min_max[-1][-1].append([min,max])
                    else:
                        min=all_routes[just_routes_2[i]]['УПК(1A,1B)']['min']
                        max=all_routes[just_routes_2[i]]['УПК(1A,1B)']['max']
                        min_max[-1][-1].append([min,max])

                        min=all_routes[just_routes_2[i]]['УПК(2A,2B)']['min']
                        max=all_routes[just_routes_2[i]]['УПК(2A,2B)']['max']
                        min_max[-1][-1].append([min,max])

                        just_routes[i][j]=['УПК1','УПК2']

                if type(just_routes[i][j])!=list:
                    min=all_routes[just_routes_2[i]][just_routes[i][j]]['min']
                    max=all_routes[just_routes_2[i]][just_routes[i][j]]['max']
                    just_routes[i][j]=[just_routes[i][j]]
                    min_max[-1][-1].append([min,max])
        return just_routes,just_coeffs,min_max

    def make_routes(route,min_max):

        def count_prev(list,x):
            count_prev=1
            for i in range(len(list)):
                if i<x:
                    count_prev=count_prev*len(list[i])
            return count_prev

        def count_rest(list,x):
            count_rest=1
            for i in range(len(list)):
                if i>x:
                    count_rest=count_rest*len(list[i])
            return count_rest

        count=1
        for i in range(len(route)):
            count=count*len(route[i])

        comb_routes=[]
        comb_min_max=[]
        for i in range(count):
            comb_routes.append([])
            comb_min_max.append([])
        count_check=0

        for j in range(len(route)):
            i=0
            for l in range(count_prev(route,j)):
                for k in range(len(route[j])):
                    for n in range(count_rest(route,j)):
                        comb_routes[i].append(route[j][k])
                        comb_min_max[i].append(min_max[j][k])
                        i+=1

        return comb_routes,comb_min_max



    def make_pers(comb_routes,comb_min_max,steel,arr,dep,coeff,times_in_roude,un,kv,col_pers,flag_min_un_route,pair):

        def make_one_comb(indexes,all_arrs,all_deps,route,min_max):
            comb=dict(route=[])
            for i in range(len(indexes)):
                comb['route'].append(dict())
                comb['route'][-1]['agr']=route[i]
                comb['route'][-1]['dep']=all_deps[indexes[i]][i]
                comb['route'][-1]['arr']=all_arrs[indexes[i]][i]
                comb['route'][-1]['max_vpo']=min_max[i][1]
                comb['route'][-1]['min_vpo']=min_max[i][0]
            return comb


        def mean_routes(p):
            all_trans=[]
            all_trans.append(p['route'][0]['arr']-p['departure_from_kv'])

            for i in range(len(p['route'])-1):
                all_trans.append(p['route'][i+1]['arr']-p['route'][i]['dep'])

            all_trans.append(p['arrival_on_un']-p['route'][-1]['dep'])

            mean=sum(all_trans)/len(all_trans)
            dif=[]
            for i in all_trans:
                dif.append(abs(i-mean))
            dif=1+int((sum(dif)/len(dif)+1)/600)    #600 как 10 мнут для корректной работы с другими коэффициентами

            return dif


        times=[]
        for i in range(len(comb_routes)):
            if pair==[]:
                times.append([times_in_roude['КВ'+str(kv)+'-'+comb_routes[i][0]]])
            else:
                if len(pair['route'])==0:
                    times.append([times_in_roude['КВ'+str(kv)+'-'+comb_routes[i][0]]])
                else:
                    times.append([times_in_roude[pair['route'][-1]['agr_name']+'-'+comb_routes[i][0]]])
            for j in range(len(comb_routes[i])-1):

                times[-1].append(times_in_roude[comb_routes[i][j]+'-'+comb_routes[i][j+1]])
            times[-1].append(times_in_roude[comb_routes[i][-1]+'-'+'УН'+str(make_one_un(un))])


        pers=[]
        count_per=0

        times_=copy.deepcopy(times)

        for t in range(len(times_)):
            if 0 not in times_[t]:
                all_arrs=[]
                all_deps=[]
                comb_times=[]

                while True:
                    sum_time=sum(times_[t])
                    for i in comb_min_max[t]:
                        sum_time+=i[0]*60
                    if sum_time<=arr-dep or max(times_[t])==0:
                        break
                    else:
                        for tt in range(len(times_[t])):
                            if times_[t][tt]>=60:
                                times_[t][tt]-=60
                            else:
                                times_[t][tt]=0


                delta=5*60

                vpo_deps=[]
                vpo_arrs=[]
                count=dep
                for i in range(len(comb_routes[t])):
                    vpo_arrs.append(count+times_[t][i])
                    vpo_deps.append(count+times_[t][i]+comb_min_max[t][i][0]*60)
                    count+=times_[t][i]+comb_min_max[t][i][0]*60


                iter_list=[]
                if sum_time<=arr-dep:
                    col=(arr-dep-sum_time)//delta
                    for i in range(col+1):
                        all_arrs.append([x+delta*i for x in vpo_arrs])
                        all_deps.append([x+delta*i for x in vpo_deps])
                        iter_list.append(i)


                if flag_min_un_route==True:
                    for i in range(len(all_arrs)):
                        all_deps[i][-1]=arr-times_[t][-1]
                        all_arrs[i][-1]=all_deps[i][-1]-comb_min_max[t][-1][0]*60



                #Создать переменные + Удалить лишние (от последнего впо до УН >30мин)
                for i in combinations_with_replacement(iter_list, len(comb_routes[t])):
                    if times[t][-1]<=1800:
                        pers.append(make_one_comb(sorted(list(i)),all_arrs,all_deps,comb_routes[t],comb_min_max[t]))
                        pers[-1]['weight']=coeff
                        pers[-1]['departure_from_kv']=dep
                        pers[-1]['arrival_on_un']=arr
                        pers[-1]['un']=make_one_un(un)
                        pers[-1]['kv']=kv
                        pers[-1]['num_per']=col_pers+count_per
                        pers[-1]['times_in_roude']=times[t]
                        pers[-1]['assortment']=steel
                        pers[-1]['weight']=pers[-1]['weight']*mean_routes(pers[-1])
                        pers[-1]['flag_min_un_route']=flag_min_un_route
                        pers[-1]['vpo_transportation_cons']=True
                        if times_[t]!=times[t]:
                            pers[-1]['vpo_transportation_cons']=False
                        if not pers[-1]['vpo_transportation_cons']:
                            pers[-1]['weight']=pers[-1]['weight']*2000
                        if pair!=[]:
                            pers[-1]['noKV']=False
                        count_per+=1



        return pers

    def find_acv_ogrs(pers):
        acv_ogrs=[]
        for i in range(len(pers)):
            if pers[i]['assortment']=='ОНУ  (Mn min < 0,45%)' or pers[i]['assortment']=='ОНУ  (Mn min ≥ 0,45%)':
                if pers[i]['weight']==1:
                    for j in pers[i]['route']:
                        if j['agr']=='АЦВ':
                            if pers[i]['arrival_on_un']-j['dep']>1800:
                                acv_ogrs.append(i)
                            break


        return acv_ogrs

    def add_flags_sols(out_sols,TO_name,TO_start,TO_finish,TO_fact_flag,vpo_message):
        vpo_message['fact']=True
        vpo_message['to']=True
        vpo_message['transportation']=True
        for i in out_sols:
            i['vpo_route_cons']=True
            i['vpo_to_cons']=True
            i['vpo_fact_cons']=True
            if not i['vpo_transportation_cons']:
                vpo_message['transportation']=False
            for j in i['route']:
                j['vpo_route_cons']=True
                j['vpo_delta_cons']=True
                j['vpo_transportation_cons']=i['vpo_transportation_cons']
                for ii in out_sols:
                    if i!=ii:
                        for jj in ii['route']:
                            if j['agr']==jj['agr']:
                                if j['arr']-10*60<jj['dep'] and j['arr']-10*60>jj['arr']:
                                    j['vpo_delta_cons']=False
                                    break
                                if j['dep']+10*60<jj['dep'] and j['dep']+10*60>jj['arr']:
                                    j['vpo_delta_cons']=False
                                    break
                    if j['vpo_delta_cons']==False:
                        break


                j['vpo_to_cons']=True
                j['vpo_fact_cons']=True

                for t in range(len(TO_name)):
                    if TO_name[t]==j['agr']:
                        if j['dep']<TO_finish[t] and j['dep']>TO_start[t]:
                            if TO_fact_flag[t]:
                                j['vpo_fact_cons']=False
                                i['vpo_fact_cons']=False
                                vpo_message['fact']=False
                            else:
                                j['vpo_to_cons']=False
                                i['vpo_to_cons']=False
                                vpo_message['to']=False
                            break
                        if j['arr']<TO_finish[t] and j['arr']>TO_start[t]:
                            if TO_fact_flag[t]:
                                j['vpo_fact_cons']=False
                                i['vpo_fact_cons']=False
                                vpo_message['fact']=False
                            else:
                                j['vpo_to_cons']=False
                                i['vpo_to_cons']=False
                                vpo_message['to']=False
                            break
        return out_sols,vpo_message




    data_in=copy.deepcopy(data_out)
    solution=copy.deepcopy(data_out)
    data_in=data_in['data_in_full']


    #входные данные + модель
    col_un=4
    col_kv=3

    #Необходимо когда появится перепланирование

    if solution['solution_future']['recalc']!=[]:
        solution=solution['solution_future']['recalc']
    else:
        solution=solution['solution_future']['plan']


    TO_in=data_in['TO']
    data_in=data_in['unrs']

    agr=[]
    steel=[]
    num=[]
    for i in range(col_un):
        agr.append([])
        steel.append([])
        num.append([])
    for i in data_in:
        if i['num']!=0:
            agr[i['un']].append(i['trip1'])
            steel[i['un']].append(i['assortment'])
            num[i['un']].append(i['num'])

    TO_ag=[]
    TO_start=[]
    TO_finish=[]
    TO_name=[]
    TO_fact_flag=[]
    with open(ROOT+"/ag_ids.json", "r") as read_file:
        ags_ids=json.load(read_file)
    for t in TO_in:
        if t['ag']>3 and t['ag']<12:
            TO_ag.append(t['ag'])
            for i in ags_ids.keys():
                if ags_ids[i]==t['ag']:
                    TO_name.append(i)
                    break
            TO_start.append(t['start'])
            TO_finish.append(t['finish'])
            if t.get('fact_flag')==True:
                TO_fact_flag.append(True)
            else:
                TO_fact_flag.append(False)




    Ogrs=[]
    vyds=[]
    Per_num1=0    #количетво переменных
    Mas_num=[]
    Mas_Weight=[]
    Mas_Dep=[]
    Mas_Arr=[]
    Mas_UN=[]
    Mas_KV=[]
    Mas_route=[]
    Mas_steel=[]
    Mas_min_route=[]
    num_count=copy.deepcopy(num)
    agr_count=copy.deepcopy(agr)
    steel_count=copy.deepcopy(steel)
    for i in solution:
        i['un']=make_one_index(i['un'])
        Mas_num.append(i['num'])
        Mas_Dep.append(i['departure_from_kv'])
        Mas_Arr.append(i['arrival_on_un'])
        Mas_UN.append(i['un'])
        Mas_KV.append(i['kv'])
        Mas_steel.append(steel_count[i['un']][0])
        Mas_route.append(agr_count[i['un']][0])
        Mas_min_route.append(i['min_vpo_un'])
        num_count[i['un']][0]-=1
        if num_count[i['un']][0]==0:
            num_count[i['un']].pop(0)
            steel_count[i['un']].pop(0)
            agr_count[i['un']].pop(0)



    with open(ROOT+'/duration_between_units.csv',encoding='utf-8') as f1:
        times_in_roude=[[],[],[]]
        duration1_reader = csv.DictReader(f1)

        times_in_roude=dict()
        for row in duration1_reader:
            times_in_roude[row['agr1']+'-'+row['agr2']]=int(row['time'])


    with open(ROOT+"/sortams-trips-coeffs.json", "r") as read_file:
        coeffs=json.load(read_file)

    col_pers=0
    pers=[]

    with open(ROOT+"/manual_wide.json", "r") as read_file:
        manual_wide=json.load(read_file)
        for i in range(len(Mas_num)):
            Ogrs.append([])
            just_routes,just_coeffs,min_max=find_routes(coeffs,manual_wide,Mas_steel[i],Mas_Dep[i],Mas_Arr[i],Mas_UN[i],Mas_KV[i])

            for j in range(len(just_routes)):
                comb_routes,comb_min_max=make_routes(just_routes[j],min_max[j])
                pers.extend(make_pers(comb_routes,comb_min_max,Mas_steel[i],Mas_Arr[i],Mas_Dep[i],just_coeffs[j],times_in_roude,Mas_UN[i],Mas_KV[i],col_pers,Mas_min_route[i],[]))
                for k in range(len(pers)-col_pers):
                    Ogrs[-1].append(col_pers+k)
                col_pers=len(pers)



        for p in noKV_pairs:
            if p['trip1']!='УНРС':
                Ogrs.append([])
                just_routes,just_coeffs,min_max=find_routes_noKV(coeffs,manual_wide,p['assortment'],p['departure_from_kv'],p['arrival_on_un'],p['un'],p['kv'],p)
                for j in range(len(just_routes)):
                    comb_routes,comb_min_max=make_routes(just_routes[j],min_max[j])
                    pers.extend(make_pers(comb_routes,comb_min_max,p['assortment'],p['arrival_on_un'],p['departure_from_kv'],just_coeffs[j],times_in_roude,p['un'],p['kv'],col_pers,False,p))
                    for k in range(len(pers)-col_pers):
                        Ogrs[-1].append(col_pers+k)
                    col_pers=len(pers)





    acv_ogrs=find_acv_ogrs(pers)



    #альтернативное предстваление всех переменных для более быстрого поиска ограничений
    find_ogrs2_list=[[],[],[],[]]   #0-route 1-index 2-dep 3-arr
    for i in range(len(pers)):
        for j in range(len(pers[i]['route'])):
            if pers[i]['route'][j]['agr'] not in find_ogrs2_list[0]:
                find_ogrs2_list[0].append(pers[i]['route'][j]['agr'])
                find_ogrs2_list[1].append([])
                find_ogrs2_list[2].append([])
                find_ogrs2_list[3].append([])
            if pers[i]['route'][j]['agr'] in find_ogrs2_list[0]:
                ind=find_ogrs2_list[0].index(pers[i]['route'][j]['agr'])
                find_ogrs2_list[1][ind].append(i)
                find_ogrs2_list[2][ind].append(pers[i]['route'][j]['dep'])
                find_ogrs2_list[3][ind].append(pers[i]['route'][j]['arr'])
    Ogrs2=[]

    for i in range(len(find_ogrs2_list[0])):
        x=zip(find_ogrs2_list[1][i],find_ogrs2_list[2][i],find_ogrs2_list[3][i])
        xs=sorted(x,key=lambda tup: tup[1])
        find_ogrs2_list[1][i]=[x[0] for x in xs]
        find_ogrs2_list[2][i]=[x[1] for x in xs]
        find_ogrs2_list[3][i]=[x[2] for x in xs]


    for i in range(len(find_ogrs2_list[0])):
        for j in range(len(find_ogrs2_list[1][i])):
            n=j+1
            arr1=find_ogrs2_list[3][i][j]-10*60      #прибавляем 10 (но только один раз чтобы учесть ограничения на производительность)
            dep1=find_ogrs2_list[2][i][j]+10*60
            part_ogr=[find_ogrs2_list[1][i][j]]
            while n<len(find_ogrs2_list[1][i]):
                arr2=find_ogrs2_list[3][i][n]
                dep2=find_ogrs2_list[2][i][n]
                if arr2>arr1 and arr2<dep1:
                    if find_ogrs2_list[1][i][n] not in part_ogr:
                        part_ogr.append(find_ogrs2_list[1][i][n])
                if dep2>arr1 and dep2<dep1:
                    if find_ogrs2_list[1][i][n] not in part_ogr:
                        part_ogr.append(find_ogrs2_list[1][i][n])

                if dep1>arr2 and dep1<dep2:
                    if find_ogrs2_list[1][i][n] not in part_ogr:
                        part_ogr.append(find_ogrs2_list[1][i][n])
                if arr1>arr2 and arr1<dep2:
                    if find_ogrs2_list[1][i][n] not in part_ogr:
                        part_ogr.append(find_ogrs2_list[1][i][n])

                n+=1
            flag=True
            if part_ogr not in Ogrs2:
                Ogrs2.append(part_ogr)

    '''
    TO_ogrs=[]
    TO_sums=[]
    for p in pers:
        sum_pers_to=0
        for r in p['route']:
            if r['agr'] in TO_name:
                for i in range(len(TO_name)):
                    if TO_name[i]==r['agr']:
                        if TO_start[i]<r['arr'] and TO_finish[i]>r['dep']:
                            sum_pers_to+=(r['dep']-r['arr'])//60
                            break
                        if r['arr']<TO_start[i] and r['dep']>TO_finish[i]:
                            sum_pers_to+=(TO_finish[i]-TO_start[i])//60
                            break

                        if r['arr']>TO_start[i] and r['arr']<TO_finish[i]:
                            sum_pers_to+=(TO_finish[i]-r['arr'])//60
                            break
                        if r['dep']>TO_start[i] and r['dep']<TO_finish[i]:
                            sum_pers_to+=(r['dep']-TO_start[i])//60
                            break
        if sum_pers_to!=0:
            TO_ogrs.append(p['num_per'])
            TO_sums.append(sum_pers_to)
    '''


    TO_ogrs=[]
    for p in pers:
        for r in p['route']:
            if r['agr'] in TO_name:
                for i in range(len(TO_name)):
                    if TO_name[i]==r['agr']:
                        if TO_start[i]>r['arr'] and TO_start[i]<r['dep']:
                            TO_ogrs.append(p['num_per'])
                            break
                        if TO_finish[i]>r['arr'] and TO_finish[i]<r['dep']:
                            TO_ogrs.append(p['num_per'])
                            break
                        if r['arr']>TO_start[i] and r['arr']<TO_finish[i]:
                            TO_ogrs.append(p['num_per'])
                            break
                        if r['dep']>TO_start[i] and r['dep']<TO_finish[i]:
                            TO_ogrs.append(p['num_per'])
                            break
    TO_ogrs=list(set(TO_ogrs))
    #TO_ogrs=[]



    if len(pers)!=0:
        status,sols,vpo_message,vpo_delta_off=find_sols_2(pers,Ogrs,Ogrs2,TO_ogrs,acv_ogrs)
    else:
        status=-1


    out_sols=[]


    for i in sols:
        out_sols.append(pers[i])
        if i in TO_ogrs:
            out_sols[-1]['vpo_to_cons']=False
        else:
            out_sols[-1]['vpo_to_cons']=True
        out_sols[-1]['vpo_delta_cons']=True
        for j in vpo_delta_off:
            if i in Ogrs2[j]:
                out_sols[-1]['vpo_delta_cons']=False
                break


    fake_to=0


    if status==1:
        if not vpo_message['delta'] or not vpo_message['to']:
            for s in out_sols:
                if not s['vpo_delta_cons'] or not s['vpo_to_cons']:
                    for r in s['route']:
                        TO_name.append(r['agr'])
                        TO_start.append(r['arr']-10*60)
                        TO_finish.append(r['dep']+10*60)
                        fake_to+=1

        out_sols=max_vpos(out_sols,TO_name,TO_start,TO_finish)

    out_sols,vpo_message=add_flags_sols(out_sols,TO_name,TO_start,TO_finish,TO_fact_flag,vpo_message)



    return out_sols,vpo_message


def make_ladle_change_(num,un,ladle_change_value,index,ROOT):

    #Если указаний по перековшовке не получено, считаем, что она по нормативу
    if ladle_change_value==None:
        ladle_change_value=0

    with open(ROOT+"/ladle_manual.json", "r") as read_file:
        ladle_manual = json.load(read_file)
    if un==0:
        min_ladle=ladle_manual['УН2']['min']
        max_ladle=ladle_manual['УН2']['max']
    if un==1:
        min_ladle=ladle_manual['УН3']['min']
        max_ladle=ladle_manual['УН3']['max']
    if un==2:
        min_ladle=ladle_manual['УН4']['min']
        max_ladle=ladle_manual['УН4']['max']
    if un==3:
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


def make_ladle_change(num,un,ladle_change_value,index,ROOT):

    #если указаний по перековшовке не получено, считаем, что она по нормативу
    if ladle_change_value==None:
        ladle_change_value=0

    #пока без внеплановых перековшовок
    ladle_change_value=0

    with open(ROOT+"/ladle_manual.json", "r") as read_file:
        ladle_manual = json.load(read_file)
    if un==0:
        min_ladle=ladle_manual['УН2']['min']
        max_ladle=ladle_manual['УН2']['max']
    if un==1:
        min_ladle=ladle_manual['УН3']['min']
        max_ladle=ladle_manual['УН3']['max']
    if un==2:
        min_ladle=ladle_manual['УН4']['min']
        max_ladle=ladle_manual['УН4']['max']
    if un==3:
        min_ladle=ladle_manual['УН6']['min']
        max_ladle=ladle_manual['УН6']['max']

    vpo_un_min=[]
    ladle_change_list=[]

    num_plavs_for_ladle_change=num+index-1-ladle_change_value
    ladle_change_num=ladle_change_value

    num_plavs_for_ladle_change=num-ladle_change_value

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



def planner_1(data,ROOT,date1,date2,list_un_kv_time):

    def optional_to(KV_num,Start_TO,Finish_TO,new_to_kv,new_to_len,Per_num1,Mas_Weight,UN_ogr,KV_ogr,TO_ogr,Mas_KV,Mas_UN,Mas_T_Dep,Mas_T_Arr,col_steel,d,Mas_Weight_for_count,blow_i,shift_interval_ogrs,filling_ogrs,shift_plan_ogrs,kv_shift_plans,Mas_cyc,Mas_steel,blow,Mas_Smax_udch,Mas_sort,Mas_min_vpo_un):
        sols=[]
        for i in range(len(d['solution'])):
            sols.append(d['solution'][i]['departure_from_kv'])
        sols.sort()
        need_time_start1=datetime.datetime.fromtimestamp(min(sols)).date()
        t=datetime.time(15,30)
        need_time_start1=datetime.datetime.combine(need_time_start1,t)
        need_time_finish1=datetime.datetime.fromtimestamp(min(sols)).date()
        t=datetime.time(17,blow_i)
        need_time_finish1=datetime.datetime.combine(need_time_finish1,t)
        need_time_start2=datetime.datetime.fromtimestamp(max(sols)).date()
        t=datetime.time(9,0)
        need_time_start2=datetime.datetime.combine(need_time_start2,t)
        need_time_finish2=datetime.datetime.fromtimestamp(max(sols)).date()
        t=datetime.time(17,blow_i)
        need_time_finish2=datetime.datetime.combine(need_time_finish2,t)


        sols=[[],[]]
        for i in range(len(d['solution'])):
            if d['solution'][i]['kv']==new_to_kv:
                if datetime.datetime.fromtimestamp(d['solution'][i]['departure_from_kv']).date()==need_time_start1.date():
                    if datetime.datetime.fromtimestamp(d['solution'][i]['departure_from_kv'])>=need_time_start1 and datetime.datetime.fromtimestamp(d['solution'][i]['departure_from_kv'])<=need_time_finish1:
                        sols[0].append(d['solution'][i]['departure_from_kv'])
                if datetime.datetime.fromtimestamp(d['solution'][i]['departure_from_kv']).date()==need_time_start2.date():
                    if datetime.datetime.fromtimestamp(d['solution'][i]['departure_from_kv'])>=need_time_start2 and datetime.datetime.fromtimestamp(d['solution'][i]['departure_from_kv'])<=need_time_finish2:
                        sols[1].append(d['solution'][i]['departure_from_kv'])
        sols[0].sort()
        sols[1].sort()

        if sols[0]==[]:
            sols[0].insert(0,need_time_start1.timestamp())
            sols[0].insert(0,need_time_start1.timestamp())
        else:
            if min(sols[0])-blow_i*60>(need_time_start1.timestamp()):
                sols[0].insert(0,need_time_start1.timestamp())
            if max(sols[0])<need_time_finish1.timestamp()-blow_i*60:
                sols[0].append(need_time_finish1.timestamp())
        if sols[1]==[]:
            sols[1].insert(0,need_time_start2.timestamp())
            sols[1].insert(0,need_time_start2.timestamp())
            if min(sols[1])-blow_i*60>need_time_start2.timestamp():
                sols[1].insert(0,need_time_start2.timestamp())
            if max(sols[1])<need_time_finish1.timestamp()-blow_i*60:
                sols[1].append(need_time_finish2.timestamp())
        sect=[[],[]]
        for i in range(len(sols[0])-1):
            sect[0].append(sols[0][i+1]-sols[0][i]-blow_i*60)
        for i in range(len(sols[1])-1):
            sect[1].append(sols[1][i+1]-sols[1][i]-blow_i*60)
        for i in range(2):
            if sect[i]==[]:
                sect[i].append(0)
        for i in range(2):
            if max(max(sect[0]),max(sect[1])) in sect[0]:
                if max(sect[0])/60>new_to_len:
                    new_to_start=sols[0][sect[0].index(max(sect[0]))]
                    new_to_finish=sols[0][sect[0].index(max(sect[0]))+1]-blow_i*60
                    n_t=dict(ag=new_to_kv,start=new_to_start,finish=new_to_finish)
                    d['new_to']=n_t
                    return 1,d
                else:
                    low_grade=need_time_start1.timestamp()
                    high_grade=need_time_finish1.timestamp()-blow_i*60
                    new_to_start=sols[0][sect[0].index(max(sect[0]))]
                    new_to_finish=sols[0][sect[0].index(max(sect[0]))+1]-blow_i*60
            if max(max(sect[0]),max(sect[1])) in sect[1]:
                if max(sect[1])/60>new_to_len:
                    new_to_start=sols[1][sect[1].index(max(sect[1]))]
                    new_to_finish=sols[1][sect[1].index(max(sect[1]))+1]-blow_i*60
                    n_t=dict(ag=new_to_kv,start=new_to_start,finish=new_to_finish)
                    d['new_to']=n_t
                    return 1,d
                else:
                    low_grade=need_time_start2.timestamp()
                    high_grade=need_time_finish2.timestamp()-blow_i*60
                    new_to_start=sols[1][sect[1].index(max(sect[1]))]
                    new_to_finish=sols[1][sect[1].index(max(sect[1]))+1]-blow_i*60
            sols_with_new_to=[]
            Start_TO_new=copy.deepcopy(Start_TO)
            Finish_TO_new=copy.deepcopy(Finish_TO)
            if new_to_start-(new_to_len*60-(new_to_finish-new_to_start))>=low_grade:
                Start_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(new_to_start-(new_to_len*60-(new_to_finish-new_to_start))))
                Finish_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(new_to_start-(new_to_len*60-(new_to_finish-new_to_start))+new_to_len*60+blow_i*60))
            else:
                Start_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(low_grade))
                Finish_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(low_grade+new_to_len*60+blow_i*60))
            new_to=dict(ag=new_to_kv,start=Start_TO_new[new_to_kv-1][len(Start_TO_new[new_to_kv-1])-1].timestamp(),finish=Finish_TO_new[new_to_kv-1][len(Finish_TO_new[new_to_kv-1])-1].timestamp()-blow_i*60)
            TO_ogr=adding_to(Start_TO_new,KV_num,Per_num1,Mas_T_Dep,Finish_TO_new,Mas_KV)
            new_status,new_data=find_sols_1(Per_num1,Mas_Weight,UN_ogr,KV_ogr,TO_ogr,Mas_KV,Mas_UN,Mas_T_Dep,Mas_T_Arr,col_steel,Mas_Weight_for_count,shift_interval_ogrs,filling_ogrs,shift_plan_ogrs,kv_shift_plans,Mas_cyc,Mas_steel,blow,Mas_Smax_udch,Mas_sort,Mas_min_vpo_un)
            new_data['new_to']=new_to
            if new_status==1:
                sols_with_new_to.append(new_data)
            Start_TO_new=copy.deepcopy(Start_TO)
            Finish_TO_new=copy.deepcopy(Finish_TO)
            if new_to_finish+(new_to_len*60-(new_to_start-new_to_finish))<=high_grade:
                Start_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(new_to_finish+(new_to_len*60-(new_to_finish-new_to_start))-new_to_len*60))
                Finish_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(new_to_finish+(new_to_len*60-(new_to_finish-new_to_start))+blow_i*60))
            else:
                Start_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(high_grade-new_to_len*60))
                Finish_TO_new[new_to_kv-1].append(datetime.datetime.fromtimestamp(high_grade+blow_i*60))
            new_to=dict(ag=new_to_kv,start=Start_TO_new[new_to_kv-1][len(Start_TO_new[new_to_kv-1])-1].timestamp(),finish=Finish_TO_new[new_to_kv-1][len(Finish_TO_new[new_to_kv-1])-1].timestamp()-blow_i*60)
            TO_ogr=adding_to(Start_TO_new,KV_num,Per_num1,Mas_T_Dep,Finish_TO_new,Mas_KV)
            new_status,new_data=find_sols_1(Per_num1,Mas_Weight,UN_ogr,KV_ogr,TO_ogr,Mas_KV,Mas_UN,Mas_T_Dep,Mas_T_Arr,col_steel,Mas_Weight_for_count,shift_interval_ogrs,filling_ogrs,shift_plan_ogrs,kv_shift_plans,Mas_cyc,Mas_steel,blow,Mas_Smax_udch,Mas_sort,Mas_min_vpo_un)
            new_data['new_to']=new_to
            if new_status==1:
                sols_with_new_to.append(new_data)
            if len(sols_with_new_to)>1:
                if sols_with_new_to[0]['summ']>sols_with_new_to[1]['summ']:
                    return 1,sols_with_new_to[0]
                else:
                    return 1,sols_with_new_to[1]
            if len(sols_with_new_to)==1:
                return 1,sols_with_new_to[0]
            if len(sols_with_new_to)>0:
                break
            else:
                sect[0][sect[0].index(max(sect[0]))]=0
                sect[1][sect[1].index(max(sect[1]))]=0
        return 1,d



    def find_sols_1(Per_num1,Mas_Weight,UN_ogr,KV_ogr,TO_ogr,Mas_KV,Mas_UN,Mas_T_Dep,Mas_T_Arr,col_steel,Mas_Weight_for_count,shift_interval_ogrs,filling_ogrs,shift_plan_ogrs,kv_shift_plans,Mas_cyc,Mas_steel,blow,Mas_Smax_udch,Mas_sort,Mas_min_vpo_un):
        col_steel2=copy.deepcopy(col_steel)
        if len(Mas_Weight_for_count)==0:
            d=dict(solution=[],summ=0,code="600")
            return 0,d
        x=[]
        for i in range(Per_num1):
            x.append(pulp.LpVariable("x"+str(i), lowBound=0,cat='Integer'))
        problem = pulp.LpProblem('0',LpMinimize)
        f=None
        for i in range(Per_num1):
            f+=x[i]*Mas_Weight_for_count[i]
        problem += f, "Функция цели"
        Count=1
        for i in range(len(UN_ogr)):
            f=None
            for j in range(len(UN_ogr[i])):
                f+=x[UN_ogr[i][j]]
            if f!=None:
                problem += f == 1, str(Count)
                Count+=1

        for i in range(len(KV_ogr)):
            f=None
            for j in range(len(KV_ogr[i])):
                f+=x[KV_ogr[i][j]]
            problem += f <= 1, str(Count)
            Count+=1

        for i in range(len(filling_ogrs)):
            f=None
            for j in range(len(filling_ogrs[i])):
                f+=x[filling_ogrs[i][j]]
            problem += f <= 2, str(Count)
            Count+=1

        for i in range(len(shift_interval_ogrs)):
            f=None
            for j in range(len(shift_interval_ogrs[i])):
                f+=x[shift_interval_ogrs[i][j]]
            problem += f <= 1, str(Count)
            Count+=1

        #ограничения на запланированное количество плавок по кв в каждой смене
        for i in range(len(shift_plan_ogrs)):
            f=None
            for j in range(len(shift_plan_ogrs[i])):
                f+=x[shift_plan_ogrs[i][j]]
            if f!=None:
                problem += f == kv_shift_plans[i], str(Count)
                Count+=1


        for i in range(Per_num1):
            if i in TO_ogr:
                problem += x[i] == 0, str(Count)
                Count+=1


        problem.solve(pulp.PULP_CBC_CMD(msg=False))

        sols=[]
        for variable in problem.variables():
            if variable.varValue==1:
                sols.append(int(variable.name[1:]))

        if problem.status==1:
            sols_for_sort=[]
            times_for_sort=[]
            for i in range(len(sols)):
                sols_for_sort.append(sols[i])
                times_for_sort.append(int(mktime(Mas_T_Dep[sols[i]].timetuple())))
            x=zip(times_for_sort,sols_for_sort)
            xs=sorted(x,key=lambda tup: tup[0])
            sols=[x[1] for x in xs]


            #Сделать список отсортированных по времени вверх списков времени приезда на ун и заполнять выход по ун из него чтобы исправить нумерацию по ун
            #-----------------------------
            un_arrs=[[],[],[],[],[]]
            for i in sols:
                un_arrs[Mas_UN[i]].append(Mas_T_Arr[i])

            for i in range(len(un_arrs)):
                un_arrs[i]=sorted(un_arrs[i])

            #------------------------

            count=[]
            for i in range(4):
                count.append(0)
            d=dict()
            sum=0
            dd=dict()
            l=[]

            #адаптация для "убитых" серий
            for i in col_steel2:
                while 1:
                    if 0 in i:
                        i.pop(i.index(0))
                    else:
                        break


            for i in range(len(sols)):
                count[Mas_UN[sols[i]]]+=1
                shift=define_shift(sols[i],shift_plan_ogrs)

                l.append(dict(kv=Mas_KV[sols[i]],un=Mas_UN[sols[i]],start_on_kv=int(mktime((Mas_T_Dep[sols[i]]).timetuple()))-blow[Mas_KV[sols[i]]-1]*60,
                departure_from_kv=int(mktime(Mas_T_Dep[sols[i]].timetuple())),arrival_on_un=int(mktime(un_arrs[Mas_UN[sols[i]]][0].timetuple())),
                finish_on_un=int(mktime(un_arrs[Mas_UN[sols[i]]][0].timetuple()))+Mas_cyc[sols[i]].seconds,num=count[Mas_UN[sols[i]]],steel=Mas_steel[sols[i]],
                cyc=Mas_cyc[sols[i]].seconds/60,vyd=(int(mktime(un_arrs[Mas_UN[sols[i]]][0].timetuple()))-int(mktime(Mas_T_Dep[sols[i]].timetuple())))//60,shift=shift,Smax_udch=Mas_Smax_udch[sols[i]],
                assortment=Mas_sort[sols[i]],id_series=Mas_id[sols[i]],min_vpo_un=Mas_min_vpo_un[sols[i]]))

                l[-1]['vyd']=(l[-1]['arrival_on_un']-l[-1]['departure_from_kv'])//60

                un_arrs[Mas_UN[sols[i]]].pop(0)
                sum+=Mas_Weight[sols[i]]
                if count[Mas_UN[sols[i]]]==col_steel2[Mas_UN[sols[i]]][0]:
                    count[Mas_UN[sols[i]]]=0
                    col_steel2[Mas_UN[sols[i]]].pop(0)

            d.update(solution=l,summ=sum,code="200")
        else:
            d=dict(solution=[],summ=0,code="600")

        return problem.status,d



    def adding_to(Start_TO,KV_num,Per_num1,Mas_T_Dep,Finish_TO,Mas_KV):
        TO_num=[]
        for i in range(len(Start_TO)):
            TO_num.append(len(Start_TO[i]))
        TO_ogr=[]
        for i in range(KV_num):
            for j in range(TO_num[i]):
                for k in range(Per_num1):
                    if Mas_KV[k]==i+1:
                        T1_comp=Mas_T_Dep[k]
                        T2_comp=Finish_TO[i][j]
                        T3_comp=Start_TO[i][j]
                        if T1_comp >= T3_comp and T1_comp <= T2_comp:
                            TO_ogr.append(k)
        return TO_ogr

    def find_weights(bins_file_reader,triples_file_reader,steel,vyd,kv,un,bins_1):

        bin=find_bin(bins_file_reader,triples_file_reader,steel,vyd,kv,un)
        for row in bins_file_reader:
            if row['grade_type']==steel and int(row['bin'])==bin:
                return int((float(row['avg_alcat'])*float(bins_1[1])+float(row['avg_el'])*float(bins_1[2])+float(row['avg_el'])*float(bins_1[3])*float(bins_1[0]))/100),bin
        return 0,1

    def to_rus(s):
        ss=copy.deepcopy(s)
        eng_letters=["E","T","Y","O","P","A","H","K","X","C","B","M"]
        rus_letters=["Е","Т","У","О","Р","А","Н","К","Х","С","В","М"]
        for i in range(len(eng_letters)):
            ss=ss.replace(eng_letters[i],rus_letters[i])
        return ss

    def to_eng(s):
        ss=copy.deepcopy(s)
        eng_letters=["E","T","Y","O","P","A","H","K","X","C","B","M"]
        rus_letters=["Е","Т","У","О","Р","А","Н","К","Х","С","В","М"]
        for i in range(len(rus_letters)):
            ss=ss.replace(rus_letters[i],eng_letters[i])
        return ss


    def find_bin(bins_file_reader,triples_file_reader,steel,vyd,kv,un):
        col_bins=21
        un_=make_one_un(un)
        for row in triples_file_reader:
            split_triple=row['triple']
            split_triple=split_triple.split("_")
            if row['grade_type']==steel and int(split_triple[0])==kv and int(split_triple[1])==un_:
                if vyd<float(row['bin_1']):
                    return 1
                for i in range(col_bins-1):
                    next_val=float(row['bin_'+str(i+1)])
                    bins_row_name='bin_'+str(i)
                    if float(row['bin_'+str(i)])<vyd and next_val>vyd:
                        return i+1
                return col_bins-1

        return 10

    def find_calc_vyd_ser(id,solution):
        vyds=[]
        for s in solution:
            if s['id_series']==id:
                vyds.append(s['vyd'])
        if vyds==[]:
            return 0,0,0

        return int(min(vyds)),int(sum(vyds)/len(vyds)),int(max(vyds))




    def weights_for_count(Mas_Weight,Mas_KV,Mas_UN,flags_for_weights,Mas_sort,ROOT):
        '''
        Mas_Weight_for_count=copy.deepcopy(Mas_Weight)
        for i in range(len(Mas_Weight)):
            if Mas_KV[i]==1 and Mas_UN[i]==1:
                Mas_Weight_for_count[i]=Mas_Weight_for_count[i]*10
            Mas_Weight_for_count[i]-=flags_for_weights[i]
        '''
        with open(ROOT+"/manual_wide.json", "r") as read_file:
            manual_wide = json.load(read_file)
        steels=manual_wide.keys()
        steels_=[]
        for s in steels:
            routes=manual_wide[s].keys()
            for r in routes:
                r.split('-')
                if r[0]=='УДМ(1,2,3)':
                    steels_.append(s)
                    break

        for i in range(len(Mas_Weight_for_count)):
            if Mas_KV[i]==1 and Mas_sort[i] in steels_:
                Mas_Weight_for_count[i]=Mas_Weight_for_count[i]*2



        return Mas_Weight_for_count

    def find_shift_ogrs(Mas_KV,Mas_T_Dep,Per_num1,blow,shift_interval,melts_interval):
        sorted_KV=[]
        sorted_Dep=[]
        sorted_indx=[]
        for i in range(Per_num1):
            sorted_KV.append(Mas_KV[i])
            sorted_Dep.append(Mas_T_Dep[i])
            sorted_indx.append(i)

        x=zip(sorted_Dep,sorted_KV,sorted_indx)
        xs=sorted(x,key=lambda tup: tup[0])
        sorted_Dep=[x[0] for x in xs]
        sorted_KV=[x[1] for x in xs]
        sorted_indx=[x[2] for x in xs]

        shift_interval_ogrs=[]

        day1_nom=day1
        day2_nom=day2
        for i in range(3):
            for j in range(Per_num1):
                part_ogr=[sorted_indx[j]]
                k=j+1
                while k<Per_num1:
                    if sorted_KV[j]==sorted_KV[k]:
                        if sorted_Dep[j].date()==day1_nom and sorted_Dep[j].time()<datetime.time(23,30) and (sorted_Dep[k]-timedelta(minutes=blow[i])).time()>datetime.time(23,30):
                            if ((sorted_Dep[k]-timedelta(minutes=blow[i]))-sorted_Dep[j])<timedelta(minutes=shift_interval[i]):
                                part_ogr.append(sorted_indx[k])
                        if sorted_Dep[j].date()==day2_nom and sorted_Dep[j].time()<datetime.time(7,30) and (sorted_Dep[k]-timedelta(minutes=blow[i])).time()>datetime.time(7,30) and (sorted_Dep[k]-timedelta(minutes=blow[i])).date()==day2_nom:
                            if ((sorted_Dep[k]-timedelta(minutes=blow[i]))-sorted_Dep[j])<timedelta(minutes=shift_interval[i]):
                                part_ogr.append(sorted_indx[k])
                    k+=1
                if len(part_ogr)>1:
                    shift_interval_ogrs.append(part_ogr)

        return shift_interval_ogrs

    def find_filling_ogrs(blow,Mas_T_Dep,Mas_KV):
        filling_ogrs=[]
        par_delay=10
        for i in range(len(Mas_T_Dep)-1):
            part_ogr=[i]
            j=i+1
            while j<len(Mas_T_Dep)-1:
                if Mas_KV[i]!=Mas_KV[j]:
                    if abs((Mas_T_Dep[i]-timedelta(minutes=blow[Mas_KV[i]-1]))-(Mas_T_Dep[j]-timedelta(minutes=blow[Mas_KV[j]-1])))<timedelta(minutes=par_delay):
                        part_ogr.append(j)
                j+=1
            if len(part_ogr)>2:
                filling_ogrs.append(part_ogr)
        return filling_ogrs

    def find_kv_plan_ogrs(Mas_T_Dep,kv_shift_plans,blow,Mas_KV):
        All_deps=copy.deepcopy(Mas_T_Dep)

        #границы смен как списки [start,finish]
        bounds=[datetime.time(15,30),datetime.time(23,30),datetime.time(7,30),datetime.time(15,30)]
        shift3=[datetime.datetime.combine(day1,bounds[0]),datetime.datetime.combine(day1,bounds[1])]
        shift1=[datetime.datetime.combine(day1,bounds[1]),datetime.datetime.combine(day2,bounds[2])]
        shift2=[datetime.datetime.combine(day2,bounds[2]),datetime.datetime.combine(day2,bounds[3])]

        shift_plan_ogrs=[[],[],[]]
        for i in range(len(All_deps)):
            if (All_deps[i]-timedelta(minutes=blow[Mas_KV[i]-1]))>=shift3[0] and (All_deps[i]-timedelta(minutes=blow[Mas_KV[i]-1]))<shift3[1]:
                shift_plan_ogrs[0].append(i)
            if (All_deps[i]-timedelta(minutes=blow[Mas_KV[i]-1]))>=shift1[0] and (All_deps[i]-timedelta(minutes=blow[Mas_KV[i]-1]))<shift1[1]:
                shift_plan_ogrs[1].append(i)
            if (All_deps[i]-timedelta(minutes=blow[Mas_KV[i]-1]))>=shift2[0] and (All_deps[i]-timedelta(minutes=blow[Mas_KV[i]-1]))<=shift2[1]:
                shift_plan_ogrs[2].append(i)

        #Если стоит 0 в пришедших планах, не делать ограничений
        for i in range(len(kv_shift_plans)):
            if kv_shift_plans[i]==0:
                shift_plan_ogrs[i]=[]

        return shift_plan_ogrs


    def define_shift(sol,shift_plan_ogrs):
        indx=-1
        for i in range(len(shift_plan_ogrs)):
            if sol in shift_plan_ogrs[i]:
                indx=i
        if indx==0:
            return "3"
        if indx==1:
            return "1"
        if indx==2:
            return "2"
        if indx==-1:
            return "-"


    def add_ladle_change_list(ser,ladle_list_i,id_max,index):
        ladle_change_list_out=[]
        uniq=list(set(ladle_list_i))
        uniq.sort() # упорядочить индексы перековошовок
        col_changes=len(uniq)-1
        for i in range(col_changes+1):

            change_i=dict()
            change_ind=ladle_list_i.index((uniq[i]))
            change_i['id_list']=id_max
            change_i['index_list']=change_ind+index
            change_i['start_list']=ser['start']+change_ind*ser['cycle']*60+uniq[i]*60

            if i==col_changes:
                change_i['num_list']=ser['num']-change_ind
            else:
                change_ind_next=ladle_list_i.index((uniq[i+1]))
                change_i['num_list']=change_ind_next-change_ind

            ladle_change_list_out.append(change_i)

            id_max+=1

        ser['ladle_change_list']=ladle_change_list_out
##      пока убрать, потому что непредсказуемо ведет себ при повторном запуске
##        if col_changes>0:
##            ser['ladle_change']=ladle_change_list_out[-1]['index_list']+ser['index']-2
##        else:
##            ser['ladle_change']=ser['ladle_change_def']


        return ser,id_max







    #global day1
    #global day2
    with open(ROOT+"/test_bins_coeffs.json", "r") as read_file:
        test_bins_coeffs = json.load(read_file)


    day1=datetime.datetime.fromtimestamp(date1).date()
    day2=datetime.datetime.fromtimestamp(date2).date()

    show_row_flag='replanning_time' in data.keys()


    id_list=[]

    date_task=dict()
    unrs_task=[]
    to_task=[]
    unrs_starts=[]
    unrs_rebuilds=[]
    unrs_cyc=[]

    cyc=[]
    re_build=[]
    UN=[]
    steel=[]
    col_steel=[]
    trip1=[]

    steel_for_tab=[]

    sortam_flags_list=[]
    vyds_from_input_list=[]
    #vpos_from_input_list=[]

    letters=[]
    sections=[]

    indexes=[]

    Smax_udch=[]

    noKV_list=[]
    show_row_list=[]

    fake_stan_list=[]
    freezed_list=[]

    vpo_un_min_list=[]
    ladle_change_list=[]
    ladle_change_for_data_in_full=[]

    for i in range(4):
        id_list.append([])
        re_build.append(0)
        UN.append([])
        steel.append([])
        col_steel.append([])
        trip1.append([])
        cyc.append([])
        unrs_starts.append([])
        unrs_rebuilds.append([])
        unrs_cyc.append([])

        steel_for_tab.append([])

        sortam_flags_list.append([])
        vyds_from_input_list.append([])

        letters.append([])
        sections.append([])

        indexes.append([])

        Smax_udch.append([])

        noKV_list.append([])
        show_row_list.append([])

        fake_stan_list.append([])
        freezed_list.append([])

        vpo_un_min_list.append([])
        ladle_change_list.append([])

        ladle_change_for_data_in_full.append([])

    Start_TO=[]
    Finish_TO=[]
    for i in range(3):
        Start_TO.append([])
        Finish_TO.append([])


    for i in range(len(data['unrs'])):

        id=data['unrs'][i]['un']

        if data['unrs'][i].get('Smax_udch')==1:
            Smax_udch[id].append(1)
        else:
            Smax_udch[id].append(0)

        noKV_list[id].append(data['unrs'][i].get('noKV'))
        if show_row_flag:
            show_row_list[id].append(data['unrs'][i].get('show_row'))
        else:
            show_row_list[id].append(data['unrs'][i].get('noKV'))

        freezed_list[id].append(data['unrs'][i].get('freezed'))
        fake_stan_list[id].append(data['unrs'][i].get('fake_stan'))

        indexes[id].append(data['unrs'][i].get('index'))

        id_list[id].append(data['unrs'][i].get('id'))
        sections[id].append(data['unrs'][i].get('section'))
        letters[id].append(data['unrs'][i].get('letter'))

        if "max_vyd" in data['unrs'][i].keys() and "min_vyd" in data['unrs'][i].keys():
            vyds_from_input_list[id].append([data['unrs'][i]['min_vyd'],data['unrs'][i]['max_vyd']])
        else:
            vyds_from_input_list[id].append([])


        steel_for_tab[id].append(data['unrs'][i]['steel_mark'])


        if 'assortment' in data['unrs'][i].keys():

            if len(data['unrs'][i]['assortment'])!=0:
                sortam_flags_list[id].append(True)
                steel[id].append(data['unrs'][i]['assortment'])
            else:
                sortam_flags_list[id].append(False)
                steel[id].append(data['unrs'][i]['steel_mark'])
        else:
            sortam_flags_list[id].append(False)
            steel[id].append(data['unrs'][i]['steel_mark'])

        if data['unrs'][i]['num']==0:
            sortam_flags_list[id][-1]=True
            if data['unrs'][i].get('assortment')==None or data['unrs'][i].get('assortment')=='':
                steel[id][-1]=''




        #Чтение пришедшего маршрута
        if 'trip1' in data['unrs'][i].keys():
            if data['unrs'][i]['trip1']!="":
                trip1[id].append(data['unrs'][i]['trip1'])
            else:
                trip1[id].append("")
        else:
            trip1[id].append("")


        #замена сортамента на Стан
        tab_cyc=timedelta(minutes=(data['unrs'][i]['cycle']))
        cyc[id].append(timedelta(minutes=data['unrs'][i]['cycle']))
        unrs_cyc[id].append(data['unrs'][i]['cycle'])
        num=(data['unrs'][i]['num'])
        col_steel[id].append(data['unrs'][i]['num'])
        start=(data['unrs'][i]['start'])
        if start!=None:
            unrs_starts[id].append(start)
            start=datetime.datetime.fromtimestamp(start)
        else:
            start=UN[id][len(UN[id])-1]+re_build[id]+cyc[id][len(cyc[id])-2]
            unrs_starts[id].append(mktime(start.timetuple()))
        if data['unrs'][i]['rebuild']!=None:
            re_build[id]=(timedelta(minutes=data['unrs'][i]['rebuild']))
            unrs_rebuilds[id].append(data['unrs'][i]['rebuild'])
        else:
            re_build[id]=0
            unrs_rebuilds[id].append(0)
        for j in range(num):
            UN[id].append(start+j*tab_cyc)

        vpo_un_min,ladle_change=make_ladle_change(num,id,data['unrs'][i].get('ladle_change'),data['unrs'][i].get('index'),ROOT)

        ladle_change_list[id].append(ladle_change)
        vpo_un_min_list[id].append(vpo_un_min)

        ladle_change_for_data_in_full[id].append(data['unrs'][i].get('ladle_change'))

    if "to" in data.keys():
        to_task=data['to']
        for i in range(len(data['to'])):
            if data['to'][i]['ag'] in [1,2,3]:
                id=data['to'][i]['ag']-1
                Start_TO[id].append(datetime.datetime.fromtimestamp(data['to'][i]['start']))
                Finish_TO[id].append(datetime.datetime.fromtimestamp(data['to'][i]['finish']))


    blow=[37,37,37]
    melts_interval=[5,5,5]
    shift_interval=[10,10,10]
    if "melting_kv" in data.keys():
        for i in data['melting_kv']:
            if i.get('blow')!=None and i.get('blow')!=0:
                blow[i['ag']-1]=i['blow']
            else:
                if 'max_blow' in i.keys():
                    blow[i['ag']-1]=i['max_blow']
            melts_interval[i['ag']-1]=i['melts_interval']
            shift_interval[i['ag']-1]=i['shift_interval']


    kv_shift_plans=[0,0,0]
    if "kv_shift_plan" in data.keys():
        kv_shift_plans=[]
        kv_shift_plans.append(data["kv_shift_plan"]["shift3"])
        kv_shift_plans.append(data["kv_shift_plan"]["shift1"])
        kv_shift_plans.append(data["kv_shift_plan"]["shift2"])


    if "filling_ogrs_flag" in data.keys():
        filling_ogrs_flag=data["filling_ogrs_flag"]
    else:
        filling_ogrs_flag=False


    steel_rus=[]
    steel_eng=[]

    #поверка поиска марки стали среди известных
    found_flag=[[],[],[],[],[]]
    for i in range(len(steel)):
        steel_rus.append([])
        steel_eng.append([])
        for j in range(len(steel[i])):
            found_flag[i].append(False)
            if sortam_flags_list[i][j]!=True:
                steel[i][j]=steel[i][j].upper().replace(" ","")
                steel_rus[i].append(to_rus(steel[i][j]))
                steel_eng[i].append(to_eng(steel[i][j]))
            else:
                steel_rus[i].append(to_rus(steel[i][j]))
                steel_eng[i].append(to_eng(steel[i][j]))


    for i in range(len(steel)):
        for j in range(len(steel[i])):
            if sortam_flags_list[i][j]!=True:
                if steel[i][j]==row['grade'].upper().replace(" ","") or steel_rus[i][j]==row['grade'].upper().replace(" ","") or steel_eng[i][j]==row['grade'].upper().replace(" ",""):
                    steel[i][j]=row['steel_code']
                    found_flag[i][j]=True
            else:
                found_flag[i][j]=True

    #поверка поиска марки стали среди известных
    wrong_steels=[]
    for i in range(len(found_flag)):
        for j in range(len(found_flag[i])):
            if found_flag[i][j]==False:
                wrong_steels.append(steel[i][j])

    if len(wrong_steels)!=0:
        return -10,wrong_steels,date_task


    with open(ROOT+"/sortams-trips-coeffs.json", "r") as read_file:
        sortams_trips_coeffs = json.load(read_file)
        for i in range(len(steel)):
            for j in range(len(steel[i])):
                if col_steel[i][j]!=0:
                    if trip1[i][j]=="":
                        for k in sortams_trips_coeffs[steel[i][j]].keys():
                            if sortams_trips_coeffs[steel[i][j]][k]==1:
                                trip1[i][j]=k
                                break



    list_of_routes=[]
    Min_Vyd=[]
    Max_Vyd=[]
    Delta_Vyd=[]
    min_vyd=[]
    max_vyd=[]
    steel_for_weights=[]
    cyc_for_pers=[]
    steel_for_pers=[]
    assorts_for_pers=[]

    vyds_flags_for_pers=[]

    Smax_udch_for_pers=[]

    id_list_for_pers=[]

    vpo_un_min_list_per=[]
    ladle_change_list_per=[]

    smaller_min_vyd_per=[]

    for i in range(len(col_steel)):
        Min_Vyd.append([])
        Max_Vyd.append([])
        Delta_Vyd.append([])
        min_vyd.append([])
        max_vyd.append([])
        steel_for_weights.append([])
        list_of_routes.append([])
        cyc_for_pers.append([])
        steel_for_pers.append([])
        assorts_for_pers.append([])
        vyds_flags_for_pers.append([])
        Smax_udch_for_pers.append([])
        id_list_for_pers.append([])
        vpo_un_min_list_per.append([])
        ladle_change_list_per.append([])
        smaller_min_vyd_per.append([])
        for j in range(len(col_steel[i])):
            min_vyd[i].append(0)
            max_vyd[i].append(0)

    if data.get("smaller_min_vyd")!=False:
        smaller_min_vyd_flag=True
    else:
        smaller_min_vyd_flag=timedelta(minutes=0)



    #Новый вариант поиска выдержек (возможны разные маршруты для одного сортамента)
    with open(ROOT+"/manual_wide.json", "r") as read_file:
        manual_wide = json.load(read_file)
        for i in range(len(steel)):
            for j in range(len(steel[i])):
                if col_steel[i][j]!=0:

                    ladle_change_list_per[i].extend(ladle_change_list[i][j])
                    vpo_un_min_list_per[i].extend(vpo_un_min_list[i][j])
                    for k in range(col_steel[i][j]):
                        id_list_for_pers[i].append(id_list[i][j])
                        steel_for_weights[i].append(steel[i][j])
                        list_of_routes[i].append(trip1[i][j])
                        steel_for_pers[i].append(steel_for_tab[i][j])
                        cyc_for_pers[i].append(cyc[i][j])
                        assorts_for_pers[i].append(steel[i][j])
                        vyds_flags_for_pers[i].append(False)
                        Smax_udch_for_pers[i].append(Smax_udch[i][j])


                    if vyds_from_input_list[i][j]!=[]:
                        if vyds_from_input_list[i][j][0]!=0:
                            min_vyd[i][j]=vyds_from_input_list[i][j][0]
                        else:
                            vyd_list=[]
                            for s in manual_wide[steel[i][j]].keys():
                                vyd_list.append(manual_wide[steel[i][j]][s]['min'])
                            min_vyd[i][j]=min(vyd_list)
                        if vyds_from_input_list[i][j][1]!=0:
                            max_vyd[i][j]=vyds_from_input_list[i][j][1]
                        else:
                            vyd_list=[]
                            for s in manual_wide[steel[i][j]].keys():
                                vyd_list.append(manual_wide[steel[i][j]][s]['max'])
                            max_vyd[i][j]=max(vyd_list)
                        for k in range(col_steel[i][j]):
                            Min_Vyd[i].append(timedelta(minutes=int(min_vyd[i][j])))
                            Max_Vyd[i].append(timedelta(minutes=int(max_vyd[i][j])))
                            Delta_Vyd[i].append(timedelta(minutes=int(max_vyd[i][j])-int(min_vyd[i][j])))

                    else:
                        vyd_list=[]
                        for s in manual_wide[steel[i][j]].keys():
                            vyd_list.append(manual_wide[steel[i][j]][s]['max'])
                            vyd_list.append(manual_wide[steel[i][j]][s]['min'])
                        max_vyd[i][j]=max(vyd_list)
                        min_vyd[i][j]=min(vyd_list)

                        for k in range(col_steel[i][j]):
                            Min_Vyd[i].append(timedelta(minutes=int(min_vyd[i][j])))
                            Max_Vyd[i].append(timedelta(minutes=int(max_vyd[i][j])))
                            Delta_Vyd[i].append(timedelta(minutes=int(max_vyd[i][j])-int(min_vyd[i][j])))

                    if smaller_min_vyd_flag:
                        min_vyd_delta=[]
                        for s in manual_wide[steel[i][j]].keys():
                            min_vyd_delta.append(manual_wide[steel[i][j]][s].get('min_ti'))

                        if len(min_vyd_delta)==0 or min_vyd_delta.count(None)!=0:
                            min_vyd_delta=min_vyd[i][j]-10
                        else:
                            min_vyd_delta=min(min_vyd_delta)
                    else:
                        min_vyd_delta=0


                    for k in range(col_steel[i][j]):
                        if timedelta(minutes=min_vyd_delta)<=Min_Vyd[i][-1]:
                            smaller_min_vyd_per[i].append(Min_Vyd[i][-1]-timedelta(minutes=min_vyd_delta))
                        else:
                            smaller_min_vyd_per[i].append(timedelta(minutes=0))


    #Определяем максимальный индекс (для ladle_change_list)
    id_max=[]
    for i in id_list:
        if len(i)!=0:
            id_max.append(max(i))
    id_max=max(id_max)+1

    for i in range(len(col_steel)):
        for j in range(len(col_steel[i])):
            unrs_task.append(dict(id=id_list[i][j],un=i,assortment=steel[i][j],steel_mark=steel_for_tab[i][j],num=col_steel[i][j],start=unrs_starts[i][j],cycle=unrs_cyc[i][j],rebuild=unrs_rebuilds[i][j],
            ladle_change_def=ladle_change_for_data_in_full[i][j],trip1=trip1[i][j].replace("КВ-","").replace("-УН",""),min_vyd=int(min_vyd[i][j]),max_vyd=int(max_vyd[i][j]),section=sections[i][j],
            letter=letters[i][j],index=indexes[i][j],Smax_udch=Smax_udch[i][j],noKV=noKV_list[i][j],show_row=show_row_list[i][j],freezed=freezed_list[i][j],fake_stan=fake_stan_list[i][j]))
            unrs_task[-1],id_max=add_ladle_change_list(unrs_task[-1],ladle_change_list[i][j],id_max,indexes[i][j])
    date_task.update(unrs=unrs_task,TO=to_task)
    with open(ROOT+'/data_in_full.json', "w") as write_file:
        json.dump(date_task,write_file,ensure_ascii=False)


    T_min_dep=[]
    for i in range(len(UN)):
        T_min_dep.append([])
        if col_steel[i]!=[]:
            for j in range(len(col_steel[i])):
                un=0
                for n in range(j):
                    un+=col_steel[i][n]
                for k in range(col_steel[i][j]):
                    fix_time=UN[i][un+k]-Max_Vyd[i][un+k]
                    T_min_dep[len(T_min_dep)-1].append(fix_time)




    KV=[1,2,3]

    TO_num=[]
    for i in range(len(Start_TO)):
        TO_num.append(len(Start_TO[i]))

    #-------Конец ИД---------
    UN_num=4
    Kol_Points=[]
    for i in range(len(UN)):
        if len(UN[i])!=0:
            Kol_Points.append(len(UN[i]))
        else:
            Kol_Points.append(0)

    T_Arr=UN
    KV_num=len(KV)
    KV_id=KV
    Prod=[]
    KV_Step=[]
    for i in range(KV_num):
        if data.get("kv_step")!=None and data.get("kv_step")!=0:
            KV_Step.append(timedelta(minutes=data['kv_step']))
        else:
            KV_Step.append(timedelta(minutes=10))
        Prod.append(timedelta(minutes=blow[i])+timedelta(minutes=melts_interval[i]))

    for i in range(len(Finish_TO)):
        for j in range(len(Finish_TO[i])):
            Finish_TO[i][j]=Finish_TO[i][j]+timedelta(minutes=blow[i])

    '''
    Count = 1
    #ввести константу Step_Number (определяемую для каждой серии согласно разнице между Min и Max выдержками), вместо массива
    Step_Number=timedelta(minutes=10)

    for i in range(KV_num):
        for j in range(UN_num):
            for k in range(Kol_Points[j]):
                Step_Number=Delta_Vyd[j][k]/KV_Step[i]
                Count=Count+int(Step_Number+1)
    Per_num1 = Count - 1
    '''
    Mas_KV = []
    Mas_UN = []
    Mas_Point_Arr = []
    Mas_T_Arr = []
    Mas_T_Dep = []

    Mas_Per = []
    Time = []
    Mas_Weight = []
    Mas_Route=[]


    Mas_cyc=[]
    Mas_steel=[]

    Mas_Smax_udch=[]

    Mas_sort=[]

    Mas_id=[]
    Mas_Weight_for_count=[]

    Mas_min_vpo_un=[]


    Count = 0


    #для расставления весов при слишком больших дельтах
    flags_for_weights=[]       #значение - число которое нужно прибьавить к весу (но в списке весов для расчета)
    prev_bin=-1    #если bin будет два раза подряд равен 20-значит надо увеличивать вес



    with open(ROOT+"/bins_1.json", "r") as read_file:
        bins = json.load(read_file)
        el_kg=0
        el_r_kg=0
        for i in bins['300']['measure']:
            if i['measure_code']==10:
                el_r_kg= i['value']
            if i['measure_code']==30:
                el_kg=i['value']
        bins_1=[el_kg,bins['100']['measure'][0]['value'],bins['200']['measure'][0]['value'],el_r_kg]


    #bins_0_2.csv
    with open(ROOT+'/bins_0.csv',encoding='utf-8') as bins_file:
        bins_file_reader=csv.DictReader(bins_file)
        with open(ROOT+'/triples.csv',encoding='utf-8') as triples_file:
            triples_file_reader=csv.DictReader(triples_file)
            for i in range(KV_num):
                for j in range(UN_num):
                    for k in range(Kol_Points[j]):

                        if Delta_Vyd[j][k]+smaller_min_vyd_per[j][k]>timedelta(minutes=180):
                            KV_Step=timedelta(minutes=20)
                        if Delta_Vyd[j][k]+smaller_min_vyd_per[j][k]>timedelta(minutes=90) and Delta_Vyd[j][k]+smaller_min_vyd_per[j][k]<=timedelta(minutes=180):
                            KV_Step=timedelta(minutes=15)
                        if Delta_Vyd[j][k]+smaller_min_vyd_per[j][k]<=timedelta(minutes=90):
                            KV_Step=timedelta(minutes=10)

                        if data.get("kv_step")!=None and data.get("kv_step")!=0:
                            KV_Step=timedelta(minutes=data["kv_step"])


                        Step_Number=int((Delta_Vyd[j][k]+smaller_min_vyd_per[j][k])/KV_Step)

                        #T_min_dep[j][k]+=timedelta(minutes=ladle_change_list_per[j][k])
                        #T_Arr[j][k]+=timedelta(minutes=ladle_change_list_per[j][k])
                        T_min_dep_iter=T_min_dep[j][k]+timedelta(minutes=ladle_change_list_per[j][k])

                        flag_make_perms=True
                        if [i+1,id_list_for_pers[j][k]] in list_un_kv_time[0]:
                            flag_make_perms=False
                            to_end=datetime.datetime.fromtimestamp(list_un_kv_time[1][list_un_kv_time[0].index([i+1,id_list_for_pers[j][k]])])


                        for l in range(Step_Number+1):

                            if not flag_make_perms:
                                if T_min_dep_iter+l*KV_Step-Prod[i]>=to_end:
                                    flag_make_perms=True

                            if T_Arr[j][k]+timedelta(minutes=ladle_change_list_per[j][k])-(T_min_dep_iter+l*KV_Step)>=Min_Vyd[j][k]-smaller_min_vyd_per[j][k] and flag_make_perms:
                                Mas_KV.append(i+1)
                                Mas_UN.append(j)
                                Mas_Point_Arr.append(k+1)
                                Mas_T_Arr.append(T_Arr[j][k]+timedelta(minutes=ladle_change_list_per[j][k]))
                                Mas_T_Dep.append(T_min_dep_iter+l*KV_Step)
                                Mas_Per.append("x"+str(Count))
                                bins_file.seek(0)
                                triples_file.seek(0)

                                weight,bin=find_weights(bins_file_reader,triples_file_reader,steel_for_weights[j][k],(Max_Vyd[j][k]-l*KV_Step).seconds/60,i+1,j,bins_1)
                                if bin==prev_bin:
                                    flags_for_weights.append(flags_for_weights[-1]+1)
                                else:
                                    flags_for_weights.append(0)
                                prev_bin=bin

                                Mas_Weight.append(weight)
                                Mas_Weight_for_count.append(int(test_bins_coeffs[str(bin)]))

                                #Проверка минимальных выдержек
                                if Mas_T_Arr[-1]-Mas_T_Dep[-1]<Min_Vyd[j][k]:
                                    Mas_Weight_for_count[-1]=Mas_Weight_for_count[-1]*2

                                Mas_Route.append(list_of_routes[j][k])

                                Mas_cyc.append(cyc_for_pers[j][k])
                                Mas_steel.append(steel_for_pers[j][k])

                                Mas_Smax_udch.append(Smax_udch_for_pers[j][k])

                                Mas_sort.append(assorts_for_pers[j][k])

                                Mas_id.append(id_list_for_pers[j][k])


                                Mas_min_vpo_un.append(vpo_un_min_list_per[j][k])

                                Count+=1


    Per_num1=Count

    #чтобы не занимать память
    vpo_times_data=None



    if filling_ogrs_flag==True:
        filling_ogrs=find_filling_ogrs(blow,Mas_T_Dep,Mas_KV)
    else:
        filling_ogrs=[]
    shift_interval_ogrs=find_shift_ogrs(Mas_KV,Mas_T_Dep,Per_num1,blow,shift_interval,melts_interval)

    Count_Constraints_TO = 0
    TO_ogr=[]
    for i in range(KV_num):
        for j in range(TO_num[i]):
            for k in range(Per_num1):
                if Mas_KV[k]==i+1:
                    T1_comp=Mas_T_Dep[k]
                    T2_comp=Finish_TO[i][j]
                    T3_comp=Start_TO[i][j]
                    if T1_comp >= T3_comp and T1_comp <= T2_comp:
                        TO_ogr.append(k)
                        Count_Constraints_TO = Count_Constraints_TO + 1




    Constr_TO = Count_Constraints_TO


    Count=0
    Sum_X=""
    UN_ogr=[]

    for i in range(UN_num):
        for j in range(Kol_Points[i]):
            UN_ogr.append([])
            for k in range(Per_num1):
                if Mas_UN[k]==i and Mas_Point_Arr[k]==j+1:
                    if Sum_X=="":
                        Sum_X="=J"+str(k+2)
                    else:
                        Sum_X=Sum_X+"+"+"J"+str(k+2)
                    UN_ogr[len(UN_ogr)-1].append(k)
            Count+=1
            Sum_X=""
    y_check=Count

    Constr_UN = Count

    Sum_2_M=[]
    Flag=[]
    for i in range(Per_num1):
        Flag.append(True)
        Sum_2_M.append(",")

    for i in range(Per_num1):
        KV=Mas_KV[i]-1
        T=Mas_T_Dep[i]

        for j in range(Per_num1):
            if i==j:
                Sum_2_M[i]=Sum_2_M[i]+str(j)+','
            T1=T
            T2=Mas_T_Dep[j]
            DF=T2-T1
            T3=Prod[KV]
            if i!=j and Mas_KV[j]==KV+1 and DF>timedelta(minutes=0) and DF<=T3:
                Sum_2_M[i]=Sum_2_M[i]+str(j)+","


    for i in range(Per_num1):
        if Flag[i]==True:
            for j in range(Per_num1):
                if i!=j:
                    if Sum_2_M[i] in Sum_2_M[j] and Flag[j]==True:
                        Flag[i]=False

    Sum_2=[]
    Constr_KV=0
    Ind_Constr=0
    KV_ogr2=[]
    for i in range(Per_num1):
        Sum_2.append("")
    for i in range(Per_num1):
        if Flag[i]==True:
            KV_ogr2.append([])
            Constr_KV+=1
            Ind_Constr=1
            for j in range(Per_num1):
                if ","+str(j)+"," in Sum_2_M[i]:
                    if Sum_2[Constr_KV]=="":
                        Sum_2[Constr_KV]="=J"+str(j+2)
                    else:
                        Sum_2[Constr_KV]=Sum_2[Constr_KV]+"+J"+str(j+2)
                    KV_ogr2[len(KV_ogr2)-1].append(j)
        if Ind_Constr==1:
            Ind_Constr=0

    Indicator=[]
    Constr_New=[]
    for i in range(Per_num1):
        Constr_New.append("")
    Count_New=0
    for i in range(Per_num1):
        Indicator.append(1)

    KV_ogr=[]
    for i in range(Per_num1):
        if Indicator[i]==1:
            KV_ogr.append([i])
            Constr_New[Count_New] = "J" + str(i+2)
            Indicator[i]=0
            j=i+1
            while j < Per_num1:
                if Mas_KV[i] == Mas_KV[j] and Mas_T_Dep[i] == Mas_T_Dep[j]:
                    KV_ogr[len(KV_ogr)-1].append(j)
                    Constr_New[Count_New] = Constr_New[Count_New] + "+j" + str(j+2)
                    Indicator[j]=0
                j+=1
            Count_New+=1

    KV_ogr.extend(KV_ogr2)

    Constr_Kol_KV = Count_New

    N=Per_num1

    if kv_shift_plans!=[]:
        shift_plan_ogrs=find_kv_plan_ogrs(Mas_T_Dep,kv_shift_plans,blow,Mas_KV)
    else:
        shift_plan_ogrs=[]

    #Mas_Weight_for_count на данный момент определяются для каждого бинса по ходу создания переменных чтобы не хранить все бинсы
    Mas_Weight_for_count=weights_for_count(Mas_Weight,Mas_KV,Mas_UN,flags_for_weights,Mas_sort,ROOT)

    status,d=find_sols_1(Per_num1,Mas_Weight,UN_ogr,KV_ogr,TO_ogr,Mas_KV,Mas_UN,Mas_T_Dep,Mas_T_Arr,col_steel,Mas_Weight_for_count,shift_interval_ogrs,filling_ogrs,shift_plan_ogrs,kv_shift_plans,Mas_cyc,Mas_steel,blow,Mas_Smax_udch,Mas_sort,Mas_min_vpo_un)



    if "optional_to" in data.keys() and status!=-1:
        new_to=[]
        for i in data['optional_to']:
            if i['ag'] in [1,2,3]:
                new_to_kv=i['ag']
                new_to_len=i['length']
                new_status,d=optional_to(KV_num,Start_TO,Finish_TO,new_to_kv,new_to_len,Per_num1,Mas_Weight,UN_ogr,KV_ogr,TO_ogr,Mas_KV,Mas_UN,Mas_T_Dep,Mas_T_Arr,col_steel,d,Mas_Weight_for_count,blow[new_to_kv-1],shift_interval_ogrs,filling_ogrs,shift_plan_ogrs,kv_shift_plans,Mas_cyc,Mas_steel,blow,Mas_Smax_udch,Mas_sort,Mas_min_vpo_un)
                if 'new_to' in d.keys():
                    if d['new_to']!=[]:
                        new_to.append(dict(ag=d['new_to']['ag'],start=d['new_to']['start'],finish=d['new_to']['finish']))
                        Start_TO[d['new_to']['ag']-1].append(datetime.datetime.fromtimestamp(d['new_to']['start']))
                        Finish_TO[d['new_to']['ag']-1].append(datetime.datetime.fromtimestamp(d['new_to']['finish']))
            d['new_to']=new_to



    for i in range(len(date_task['unrs'])):
        min_calc_vyd,average_calc_vyd,max_calc_vyd=find_calc_vyd_ser(date_task['unrs'][i]['id'],d['solution'])
        date_task['unrs'][i]["calc_vyd"]=str(min_calc_vyd)+"/"+str(average_calc_vyd)+"/"+str(max_calc_vyd)
        for j in date_task['unrs'][i]['ladle_change_list']:
            j['calc_vyd']=date_task['unrs'][i]["calc_vyd"]


    return status,d,date_task

def make_indexes_from_uns(date_task):
    for i in date_task['unrs']:
        i['un']-=2
        if i['un']==4:
            i['un']-=1
    return date_task

def make_uns_from_indexes(out):
    for i in out:
        i['un']+=2
        if i['un']==5:
            i['un']+=1
    return out


def add_flags(data_out,sol_type):
    if data_out['solution_operational'][sol_type]!=[]:
        for i in data_out['solution_future'][sol_type]:
            i['vpo_route_cons']=False
            i['vpo_delta_cons']=True
            i['vpo_to_cons']=True
            i['vpo_transportation_cons']=True
            for j in data_out['solution_operational'][sol_type]:
                if i['kv']==j['kv'] and i['departure_from_kv']==j['departure_from_kv']:
                    i['vpo_to_cons']=j['vpo_to_cons']
                    i['vpo_delta_cons']=j['vpo_delta_cons']
                    i['vpo_route_cons']=True
                    i['vpo_fact_cons']=j['vpo_fact_cons']
                    i['vpo_transportation_cons']=j['vpo_transportation_cons']
                    break
    else:
        for i in data_out['solution_future'][sol_type]:
            i['vpo_to_cons']=True
            i['vpo_delta_cons']=True
            i['vpo_route_cons']=True
    return data_out



def add_indexes(data_out,sol_type):
    #Добавить id_list в solution_future
    for i in data_out['solution_future'][sol_type]:
        for j in data_out['data_in_full']['unrs']:
            if i['id_series']==j['id']:
                for k in j['ladle_change_list']:
                    if i['num']>=k['index_list'] and i['num']<k['index_list']+k['num_list']:
                        i['id_list']=k['id_list']
                        break

    for i in data_out['solution_operational'][sol_type]:
        for j in data_out['solution_future'][sol_type]:
            if i['kv']==j['kv'] and i['departure_from_kv']==j['departure_from_kv']:
                i['id_series']=j['id_series']
                i['num']=j['num']
                i['plav_id']=j['plav_id']
                i['steel']=j['steel']
                i['vyd']=j['vyd']
                i['id_list']=j['id_list']
                break
        i.pop('num_per')
    return data_out

def rename_agrs(data_out,ags_ids,sol_type):
    for i in data_out['solution_operational'][sol_type]:
        for j in i['route']:
            j['agr_code']=ags_ids[j['agr']]

    return data_out


def calc_plans(data_in_full,fact_sols,sols,blow,day1,day2):

    bounds=[datetime.time(15,30),datetime.time(23,30),datetime.time(7,30),datetime.time(15,30)]
    shift3=[datetime.datetime.combine(day1,bounds[0]),datetime.datetime.combine(day1,bounds[1])]
    shift1=[datetime.datetime.combine(day1,bounds[1]),datetime.datetime.combine(day2,bounds[2])]
    shift2=[datetime.datetime.combine(day2,bounds[2]),datetime.datetime.combine(day2,bounds[3])]

    shift_plan_ogrs=[0,0,0]
    for i in sols:
        if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift3[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<shift3[1]:
            shift_plan_ogrs[0]+=1
        if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift1[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<shift1[1]:
            shift_plan_ogrs[1]+=1
        if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift2[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<=shift2[1]:
            shift_plan_ogrs[2]+=1

    shift_fact_ogrs=[0,0,0]
    for i in fact_sols:
        kv_start=i['arrival_on_kv']
        if kv_start==None or kv_start==0:
            kv_start=i['departure_from_kv']-blow[i['kv']-1]*60
        kv_start=datetime.datetime.fromtimestamp(kv_start)
        if kv_start>=shift3[0] and kv_start<shift3[1]:
            shift_fact_ogrs[0]+=1
        if kv_start>=shift1[0] and kv_start<shift1[1]:
            shift_fact_ogrs[1]+=1
        if kv_start>=shift2[0] and kv_start<=shift2[1]:
            shift_fact_ogrs[2]+=1

    #сделать из этого строку
    out_plans=[]
    for i in range(3):
        out_plans.append(str(shift_plan_ogrs[i]+shift_fact_ogrs[i])+' ('+str(shift_fact_ogrs[i])+'/'+str(shift_plan_ogrs[i])+')')


    return out_plans

def count_Smax_udch(data_in_full,fact_sols,sols,blow,day1,day2):

    bounds=[datetime.time(15,30),datetime.time(23,30),datetime.time(7,30),datetime.time(15,30)]
    shift3=[datetime.datetime.combine(day1,bounds[0]),datetime.datetime.combine(day1,bounds[1])]
    shift1=[datetime.datetime.combine(day1,bounds[1]),datetime.datetime.combine(day2,bounds[2])]
    shift2=[datetime.datetime.combine(day2,bounds[2]),datetime.datetime.combine(day2,bounds[3])]

    Smax_udch_plan=[0,0,0]
    for i in sols:
        if i['Smax_udch']==1:
            if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift3[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<shift3[1]:
                Smax_udch_plan[0]+=1
            if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift1[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<shift1[1]:
                Smax_udch_plan[1]+=1
            if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift2[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<=shift2[1]:
                Smax_udch_plan[2]+=1
        i.pop('Smax_udch')

    Smax_udch_fact=[0,0,0]
    for i in fact_sols:
        if i.get('Smax_udch')==1:
            if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift3[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<shift3[1]:
                Smax_udch_fact[0]+=1
            if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift1[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<shift1[1]:
                Smax_udch_fact[1]+=1
            if (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))>=shift2[0] and (datetime.datetime.fromtimestamp(i['departure_from_kv'])-timedelta(minutes=blow[i['kv']-1]))<=shift2[1]:
                Smax_udch_fact[2]+=1

    #сделать из этого строку
    out_plans=[]
    for i in range(3):
        out_plans.append(str(Smax_udch_plan[i]+Smax_udch_fact[i])+' ('+str(Smax_udch_fact[i])+'/'+str(Smax_udch_plan[i])+')')


    return out_plans

def add_num_melt(fact,solution):
    max_num_melt=0
    for i in fact:
        if int(i['num_melt'])>max_num_melt:
            max_num_melt=int(i['num_melt'])
    if max_num_melt>0:
        num_melt=max_num_melt+1
        for i in solution:
            i['num_melt']=num_melt
            num_melt+=1
    else:
        for i in solution:
            i['num_melt']=0
    return solution

def add_plav_ids_to_plan(solution_future):
    for i in range(len(solution_future)):
        solution_future[i]['plav_id']=i+1


def adapt_max_vyd(data_def,data_calc):
    for i in range(len(data_def)):
        calc=data_calc[i]['calc_vyd'].split('/')
        max_calc=int(calc[2])
        if data_calc[i].get('fake_stan')!=True:
            if max_calc>data_def[i]['max_vyd']/1.5:
                data_calc[i]['max_vyd']=max_calc
            else:
                data_calc[i]['max_vyd']=int(data_def[i]['max_vyd']//1.5)

    return data_calc

def adapt_indexes(task):
    for i in task:
        if i['num']==0:
            i['index']=1

def make_one_un(un):
    un+=2
    if un==5:
        un+=1
    return un

def make_fact_for_to(fact,replanning_time):
    fact_for_to=[]
    for i in fact:
        kv_start=find_kv_start(i['route'])
        if kv_start<=replanning_time:
            fact_for_to.append(i)

    return fact_for_to

def find_kv_start(route):
    kv_start=[]
    for i in route:
        if i['agr_name']=='kv':
            if i['agr_code']!=None:
                kv_start.append(int(i['start']))

    if len(kv_start)==0:
        kv_start=[0]
    return max(kv_start)

def pop_future_plavs_from_fact(fact,replanning_time):
    #создание списка плавок которые нужно уждалить
    noKV_fact=[]
    plavs_to_pop=[]
    for i in fact:

        un_start,un_num,un_finish,kv_num,kv_finish=define_un_kv(i['route'])


        if i['sign_planner']==0:
            plavs_to_pop.append(i)


        flag=True
        for r in i['route']:
            if r['agr_name']=='un':
                flag=False
                break
        if flag:
            noKV_fact.append(i)


    #обрезание факта
    for i in plavs_to_pop:
        fact.pop(fact.index(i))


    for i in noKV_fact:
        if i in fact:
            fact.pop(fact.index(i))

    return fact,noKV_fact

def sort_fact(fact):
    new_fact=copy.deepcopy(fact)
    times_for_sort=[]
    for i in fact:
        un_start,un_num,un_finish,kv_num,kv_finish=define_un_kv(i['route'])
        times_for_sort.append(un_start)
    x=zip(times_for_sort,new_fact)
    xs=sorted(x,key=lambda tup: tup[0])
    fact=[x[1] for x in xs]
    return fact

def define_un_kv_2(route):
    kv_num=[]
    kv_finish=[]
    un_num=[]
    un_finish=[]
    un_start=[]
    kv_start=[]
    for i in route:
        if i['agr_name']=='kv':
            if i['agr_code']!=None and i['finish']!=None:
                kv_num.append(int(i['agr_code']))
                kv_finish.append(int(i['finish']))
                kv_start.append(int(i['start']))
        if i['agr_name']=='un':
            un_num.append(int(i['agr_code'])-12)
            un_finish.append(int(i['finish']))
            un_start.append(int(i['start']))

    if len(kv_num)==0:
        kv_finish=None
        kv_num=None
        kv_start=None
        return max(un_start),un_num[un_start.index(max(un_start))],un_finish[un_start.index(max(un_start))],kv_num,kv_finish,kv_start
    else:
        if len(un_num)==0:
            un_num=[0]
            un_finish=[0]
            un_start=[0]
        return max(un_start),un_num[un_start.index(max(un_start))],un_finish[un_start.index(max(un_start))],kv_num[kv_finish.index(max(kv_finish))],max(kv_finish),kv_start[kv_finish.index(max(kv_finish))]


def define_un_kv(route):
    kv_num=[]
    kv_finish=[]
    un_num=[]
    un_finish=[]
    un_start=[]
    for i in route:
        if i['agr_name']=='kv':
            if i['agr_code']!=None and i['finish']!=None:
                kv_num.append(int(i['agr_code']))
                kv_finish.append(int(i['finish']))
        if i['agr_name']=='un':
            un_num.append(int(i['agr_code'])-12)
            un_finish.append(int(i['finish']))
            un_start.append(int(i['start']))

    if len(kv_num)==0:
        kv_finish=None
        kv_num=None
        return max(un_start),un_num[un_start.index(max(un_start))],un_finish[un_start.index(max(un_start))],kv_num,kv_finish
    else:
        if len(un_num)==0:
            un_num=[0]
            un_finish=[0]
            un_start=[0]
        return max(un_start),un_num[un_start.index(max(un_start))],un_finish[un_start.index(max(un_start))],kv_num[kv_finish.index(max(kv_finish))],max(kv_finish)


def associate_id_indexes(data):
    id_indexes=[[],[],[]]
    for i in data:
        id_indexes[0].append(i['id'])
        id_indexes[1].append(i['un'])
        id_indexes[2].append([i['index'],i['index']+i['num']])
    return id_indexes


def change_kv_shift_plan_for_replanninig(kv_shift_plan,plan,blow):
    #границы смен как списки [start,finish]
    day1_date=datetime.datetime.fromtimestamp(day1_timestamp).date()
    day2_date=datetime.datetime.fromtimestamp(day2_timestamp).date()
    bounds=[datetime.time(15,30),datetime.time(23,30),datetime.time(7,30),datetime.time(15,30)]
    shift3=[datetime.datetime.combine(day1_date,bounds[0]),datetime.datetime.combine(day1_date,bounds[1])]
    shift1=[datetime.datetime.combine(day1_date,bounds[1]),datetime.datetime.combine(day2_date,bounds[2])]
    shift2=[datetime.datetime.combine(day2_date,bounds[2]),datetime.datetime.combine(day2_date,bounds[3])]


    for i in plan:
        kv_start=i['arrival_on_kv']
        if kv_start==None or kv_start==0:
            kv_start=i['departure_from_kv']-blow[i['kv']-1]*60
        kv_start=datetime.datetime.fromtimestamp(kv_start)
        if kv_start>=shift3[0] and kv_start<shift3[1]:
            if kv_shift_plan['shift3']!=0:
                kv_shift_plan['shift3']-=1
        if kv_start>=shift1[0] and kv_start<shift1[1]:
            if kv_shift_plan['shift1']!=0:
                kv_shift_plan['shift1']-=1
        if kv_start>=shift2[0] and kv_start<=shift2[1]:
            if kv_shift_plan['shift2']!=0:
                kv_shift_plan['shift2']-=1

    return kv_shift_plan


def define_shift(t,day1,day2):
    day1_date=datetime.datetime.fromtimestamp(day1).date()
    day2_date=datetime.datetime.fromtimestamp(day2).date()
    bounds=[datetime.time(15,30),datetime.time(23,30),datetime.time(7,30),datetime.time(15,30)]
    shift3=[datetime.datetime.combine(day1_date,bounds[0]),datetime.datetime.combine(day1_date,bounds[1])]
    shift1=[datetime.datetime.combine(day1_date,bounds[1]),datetime.datetime.combine(day2_date,bounds[2])]
    shift2=[datetime.datetime.combine(day2_date,bounds[2]),datetime.datetime.combine(day2_date,bounds[3])]

    shift_plan_ogrs=[[],[],[]]
    if datetime.datetime.fromtimestamp(t)>=shift3[0] and datetime.datetime.fromtimestamp(t)<shift3[1]:
        return "3"
    if datetime.datetime.fromtimestamp(t)>=shift1[0] and datetime.datetime.fromtimestamp(t)<shift1[1]:
        return "1"
    if datetime.datetime.fromtimestamp(t)>=shift2[0] and datetime.datetime.fromtimestamp(t)<=shift2[1]:
        return "2"
    return "-"


def add_bigger_right_bounds_flags(data_def,data_out,bigger_right_bounds,sol_type):
    max_vyds_def={}
    for d in data_def:
        max_vyds_def[d['id']]=d['max_vyd']
    if bigger_right_bounds:
        for p in data_out['solution_future'][sol_type]:
            if p['vyd']>max_vyds_def[p['id_series']]:
                p['bigger_right_bound']=True
            else:
                p['bigger_right_bound']=False
    else:
        for p in data_out['solution_future'][sol_type]:
            p['bigger_right_bound']=False
    return data_out

def sort_date_task(unrs):
    times_for_sort=[]
    for s in unrs:
        times_for_sort.append(s['start'])
    x=zip(times_for_sort,unrs)
    xs=sorted(x,key=lambda tup: tup[0])
    unrs=[x[1] for x in xs]
    return unrs

def make_first_plavs_to(unrs,fact):
    kv_plavs_starts=[[],[],[]]
    kv_plavs_finishes=[[],[],[]]
    kv_plavs_uns=[[],[],[]]
    for f in fact:
        un_start,un_num,un_finish,kv_num,kv_finish,kv_start=define_un_kv_2(f['route'])
        if kv_num in [1,2,3]:
            kv_plavs_starts[kv_num-1].append(kv_start)
            kv_plavs_finishes[kv_num-1].append(kv_finish)
            kv_plavs_uns[kv_num-1].append(un_num)

    for i in range(3):
        if kv_plavs_starts[i]!=[]:
            ind=kv_plavs_starts[i].index(max(kv_plavs_starts[i]))
            kv_plavs_starts[i]=kv_plavs_starts[i][ind]
            kv_plavs_finishes[i]=kv_plavs_finishes[i][ind]
            kv_plavs_uns[i]=kv_plavs_uns[i][ind]

    list_un_kv_time=[[],[]]   #0-[[kv,id_ser]]  1-[time]

    for i in range(3):
        for j in range(3):
            if i!=j and kv_plavs_starts[i]!=[] and kv_plavs_starts[j]!=[]:
                if kv_plavs_starts[i]>kv_plavs_finishes[j]:
                    id_ser=None
                    for ser in unrs:
                        if kv_plavs_uns[i]==ser['un'] and ser['num']!=0:
                            id_ser=ser['id']
                            break
                    if id_ser!=None:
                        list_un_kv_time[0].append([j+1,id_ser])
                        list_un_kv_time[1].append(kv_plavs_starts[i])


    return list_un_kv_time

def planner_1_2(input_data):


    input_data['unrs']=sort_date_task(input_data['unrs'])

    def end_planner_replanning():
        data_out['task_name']=input_data.get('task_name')


        with open(ROOT+"/data_out.json", "w") as write_file:
            json.dump(data_out,write_file,ensure_ascii=False)

        data_out['input_data']=input_data_def

        if ip!=None and log_file!=None:
            data_out['fact']=data['fact']
            data_out['replanning_time']=data['replanning_time']
            with open(ROOT+"/logs"+"/logs.txt", "a") as write_file:
                log=dict(ip=ip,task_name=log_file,time=time_str,replanning=True,replanning_time=str(datetime.datetime.fromtimestamp(data['replanning_time'])))
                write_file.write("\n"+str(log))
            with open(ROOT+"/logs"+"/"+log_file+"_replanning_time:"+time_str+".json", "w") as write_file:
                data_out['name']=log_file+"_replanning_time:"+time_str+".json"
                json.dump(data_out,write_file,ensure_ascii=False)

        with open(ROOT+"/statuses/"+status_file, "r") as read_file:
            status_data = json.load(read_file)
            status_data['stop']=2
            if data_out.get('code')=='400':
                status_data['message']='Ошибка ввода'
            else:
                if len(data_out['solution_future']['recalc'])!=0:
                    if len(data_out['solution_operational']['recalc'])!=0:
                        status_data['message']='Решение есть'
                        if data_out.get('code')=='500':
                            status_data['message']='Решения для ВПО найдено с нарушениями ограничений'
                    else:
                        status_data['message']='Решение для ВПО не найдено -- попробуйте увеличить нижнюю границу выдержек'
                else:
                    status_data['message']='Решение не найдено -- попробуйте скорректировать начало серий'
            with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                json.dump(status_data,write_file,ensure_ascii=False)

            print(log_file+"_replanning_time:"+time_str+".json")

    input_data_def=copy.deepcopy(input_data)

    log_file=input_data.get('task_name')
    ip=input_data.get('ip')
    status_file=input_data.get('status_file')

    time_str=datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

    ROOT = os.path.dirname(os.path.abspath(__file__))

    data=make_indexes_from_uns(input_data)

    #глобальные blow
    blow=[37,37,37]
    if "melting_kv" in data.keys():
        for i in data['melting_kv']:
            if i.get('blow')!=None and i.get('blow')!=0:
                blow[i['ag']-1]=i['blow']
                if i.get('max_blow')==None or i.get('max_blow')==0:
                    i['max_blow']=37
                if i.get('min_blow')==None or i.get('min_blow')==0:
                    i['min_blow']=35
            else:
                if 'max_blow' in i.keys():
                    blow[i['ag']-1]=i['max_blow']
                    i['blow']=i['max_blow']

    data_out=dict(data_in_full=dict(TO=[],unrs=[]),task_name=input_data.get('task_name'),solution_future=dict(plan=[],recalc=[]),solution_operational=dict(plan=[],recalc=[]),vpo_message=dict(route=True,delta=True,to=True,fact=True),melting_kv=data['melting_kv'])

    list_un_kv_time=[[],[]]

    if 'replanning_time' in data.keys():

        if data.get('fact')==None or data.get('fact')==[]:
            data['fact']=[]
            data_out['code']='400'
            end_planner_replanning()
            return


        #удаление плавок из будущего (теперь плавок где sign_planner=0)
        fact_for_to=make_fact_for_to(data['fact'],data['replanning_time'])
        #fact_for_to=[]
        data['fact'],noKV_fact=pop_future_plavs_from_fact(data['fact'],data['replanning_time'])
        data['fact']=sort_fact(data['fact'])



        date_task=copy.deepcopy(data)



        global day1_timestamp
        global day2_timestamp

        if 'date_start' in date_task.keys():
            try:
                day1_timestamp=int(float(data['date_start']))+3*60*60
                day2_timestamp=day1_timestamp+86400
            except:
                day1_timestamp=int(mktime(datetime.datetime.strptime(data['date_start'].split('T')[0], '%Y-%m-%d').timetuple()))
                day1_timestamp+=int(data['date_start'].split('T')[1].split(':')[0])*60*60+int(data['date_start'].split('T')[1].split(':')[1])+3*60*60
                day2_timestamp=day1_timestamp+86400
        else:
            #определение суток
            if datetime.datetime.fromtimestamp(date_task['replanning_time']).time()>datetime.time(15,30):
                day1_timestamp=date_task['replanning_time']
                day2_timestamp=day1_timestamp+86400
            else:
                day2_timestamp=date_task['replanning_time']
                day1_timestamp=day2_timestamp-86400

        #обновление суточного задания

        deps_from_kv_for_to=[[],[],[]]
        deps_from_vpo_for_to=[[],[]]   #0-vpo      1-departure from vpo

        #чтение факта
        col_made_plavs=[0,0,0,0,0]
        last_plav_times=[0,0,0,0,0]

        #список флагов для старта отсчета плавок из факта по каждому ун
        max_index=[0,0,0,0]

        #обнулить индексы у пустых серий
        adapt_indexes(date_task['unrs'])

        #list_for_nums_adapt:    0-ун для которого нужно прибавлять значения    1-какое число нужно прибавить к каждой плавке 2-сколько раз прибавить
        list_for_nums_adapt=[[],[],[]]

        for ser in date_task['unrs']:

            made_plavs=[]
            last_plav=0
            plav_finishes=[]
            start_plav=ser['start']
            start_plav2=ser['start']
            vpo_un_min,ladle_change_list=make_ladle_change(ser['num'],ser['un'],ser.get('ladle_change'),ser['index'],ROOT)
            for i in range(ser['num']):
                made_plavs.append(False)
                start_=start_plav+ladle_change_list[i]*60

                #start_plav2+=ladle_change_list[i]*60
                #поиск соответствующей плавки в факте
                for f in date_task['fact']:
                    un_start,un_num,un_finish,kv_num,kv_finish=define_un_kv(f['route'])

                    if un_num==ser['un'] and i+ser['index']==f['num_melt_series']:
                        if abs(un_start-start_)<=5400 or abs(un_start-start_plav2)<=5400:
                            made_plavs[-1]=True
                            plav_finishes.append(un_finish)
                            last_plav=un_finish
                            start_plav=last_plav
                            break
                if made_plavs[-1]==False:
                    if start_<data['replanning_time']:
                        made_plavs[-1]=True
                        plav_finishes.append(start_+ser['cycle']*60)
                    start_plav+=ser['cycle']*60
                start_plav2+=ser['cycle']*60
            #count_plavs=made_plavs.count(True)
            #print(ser['id'],ser['un'],made_plavs,ser['num'])


            count_plavs=[p for p in range(len(made_plavs)) if made_plavs[p]]
            if count_plavs!=[]:
                count_plavs=max(count_plavs)+1
            else:
                count_plavs=0

            if count_plavs!=0:

                ser['index']+=count_plavs
                ser['num']-=count_plavs
                ser['start']=max(plav_finishes)

            if ser['num']!=0:
                list_for_nums_adapt[0].append(ser['id'])
                list_for_nums_adapt[1].append(ser['index']-1)
                list_for_nums_adapt[2].append(ser['num'])


        list_un_kv_time=make_first_plavs_to(date_task['unrs'],data['fact'])


        #Обнуление noKV серий
        data_in_full_noKV_nums=dict()
        for i in range(len(date_task['unrs'])):
            if date_task['unrs'][i].get('noKV')==False or date_task['unrs'][i].get('show_row')==False:
                data_in_full_noKV_nums[date_task['unrs'][i]['id']]=date_task['unrs'][i]['num']
                date_task['unrs'][i]['num']=0


        for i in fact_for_to:
            un_start,un_num,un_finish,kv_num,kv_finish=define_un_kv(i['route'])
            if kv_num!=None and kv_finish!=None:
                deps_from_kv_for_to[kv_num-1].append(kv_finish)
            for j in i['route']:
                if j['agr_name']!='un' and j['agr_name']!='kv':
                    if j['agr_code'] not in deps_from_vpo_for_to[0]:
                        deps_from_vpo_for_to[0].append(j['agr_code'])
                        deps_from_vpo_for_to[1].append([])
                    deps_from_vpo_for_to[1][deps_from_vpo_for_to[0].index(j['agr_code'])].append(j['finish'])

        '''
        #----------------------
        for i in range(len(date_task['unrs'])):
            if max_index[date_task['unrs'][i]['un']]==0 and date_task['unrs'][i]['num']!=0:
                max_index[date_task['unrs'][i]['un']]=date_task['unrs'][i]['index']

        for i in date_task['fact']:
            un_start,un_num,un_finish,kv_num,kv_finish=define_un_kv(i['route'])

            if i['num_melt_series']>=max_index[un_num]:

                for j in i['route']:
                    if j['agr_name']!='un' and j['agr_name']!='kv':
                        if j['agr_code'] not in deps_from_vpo_for_to[0]:
                            deps_from_vpo_for_to[0].append(j['agr_code'])
                            deps_from_vpo_for_to[1].append([])
                        deps_from_vpo_for_to[1][deps_from_vpo_for_to[0].index(j['agr_code'])].append(j['finish'])

                col_made_plavs[un_num]+=1

                last_plav_times[un_num]=un_finish



        #list_for_nums_adapt:    0-ун для которого нужно прибавлять значения    1-какое число нужно прибавить к каждой плавке 2-сколько раз прибавить
        list_for_nums_adapt=[[],[],[]]

        id_indexes=associate_id_indexes(date_task['unrs'])



        #костыль для адаптации индексов-  if i['num']!=0: вынесено из   if col_made_plavs[i['un']-5]!=0:  (как сверху)
        #изменение суточного задния

        for i in date_task['unrs']:
            col_made_plavs_step=0
            if col_made_plavs[i['un']]!=0:

                if i['num']>=col_made_plavs[i['un']]:
                    i['num']-=col_made_plavs[i['un']]
                    i['index']+=col_made_plavs[i['un']]

                    i['start']=last_plav_times[i['un']]

                    col_made_plavs_step=col_made_plavs[i['un']]

                    col_made_plavs[i['un']]=0


                else:
                    col_made_plavs[i['un']]-=i['num']
                    i['num']=0
                    i['index']=0
            if i['num']!=0:
                list_for_nums_adapt[0].append(i['un'])
                list_for_nums_adapt[1].append(col_made_plavs_step+id_indexes[2][id_indexes[0].index(i['id'])][0]-1)
                list_for_nums_adapt[2].append(i['num'])

        #---------------------
        '''
        #изменение ТО
        for i in range(3):
            if deps_from_kv_for_to[i]!=[]:
                date_task['to'].append(dict(ag=i+1,start=max(deps_from_kv_for_to[i])-170000,finish=max(deps_from_kv_for_to[i]),fact_flag=True))
        for i in range(len(deps_from_vpo_for_to[0])):
            if deps_from_vpo_for_to[1][i]!=[]:
                date_task['to'].append(dict(ag=deps_from_vpo_for_to[0][i],start=max(deps_from_vpo_for_to[1][i])-170000,finish=max(deps_from_vpo_for_to[1][i]),fact_flag=True))

        fact_plavs=[]
        for i in fact_for_to:
            un_start,un_num,un_finish,kv_num,kv_finish,kv_start=define_un_kv_2(i['route'])
            if kv_num!=None and kv_finish!=None:
                fact_plavs.append(dict(kv=kv_num,un=un_num,departure_from_kv=kv_finish,arrival_on_un=un_start,arrival_on_kv=kv_start))


        date_task['kv_shift_plan']=change_kv_shift_plan_for_replanninig(date_task['kv_shift_plan'],fact_plavs,blow)
        with open(ROOT+"/statuses/"+status_file, "r") as read_file:
            status_data = json.load(read_file)
            status_data['message']='Поиск решения'
            status_data['code']=10
            with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                json.dump(status_data,write_file,ensure_ascii=False)

        try:
            status, solution,data_in_for_2alg=planner_1(date_task,ROOT,day1_timestamp,day2_timestamp,list_un_kv_time)
        except:
            status=400
            data_in_for_2alg=dict(unrs=[],TO=[])
            solution=dict()
            solution['solution']=[]
            solution['code']="400"
            solution['summ']=0
            data_out['code']="400"

        #для адаптации правых границ после всех манипуляций
        data_in_def=copy.deepcopy(data_in_for_2alg)

        bigger_right_bounds=False


        #циклическое увеличение правых границ в полтора раза
        if status==-1:
            bigger_right_bounds=True
            date_task.update(unrs=data_in_for_2alg['unrs'])
            out_with_message=dict(data_in_full=[],solution_future=dict(plan=[],recalc=[]),summ=0,code="100",message="Правые границы выдержек увеличены",new_to=[],solution_operational=dict(plan=[],recalc=[]))
            for i in range(1):
                with open(ROOT+"/statuses/"+status_file, "r") as read_file:
                    status_data = json.load(read_file)
                    status_data['message']='Поиск решения с увеличенными правыми границами'
                    status_data['code']=20
                    with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                        json.dump(status_data,write_file,ensure_ascii=False)
                for j in range(len(date_task['unrs'])):
                    if data_in_for_2alg['unrs'][j].get('fake_stan')!=True:
                        date_task['unrs'][j].update(max_vyd=data_in_for_2alg['unrs'][j]['max_vyd']*1.5,min_vyd=data_in_for_2alg['unrs'][j]['min_vyd'])
                if date_task['unrs']!=input_data['unrs']:
                    status, solution,data_in_for_2alg=planner_1(date_task,ROOT,day1_timestamp,day2_timestamp,list_un_kv_time)
                if status==1:
                    break

        if len(solution['solution'])==0 and status!=400:
            data_out['code']="600"
            data_out['summ']=0

        if status==1:

            #Возвращение num у noKV серий
            if len(data_in_full_noKV_nums.keys())!=0:
                for i in range(len(data_in_for_2alg['unrs'])):
                    if data_in_for_2alg['unrs'][i]['id'] in data_in_full_noKV_nums.keys() and data_in_for_2alg['unrs'][i]['num']==0 and data_in_for_2alg['unrs'][i]['show_row']==True:
                        data_in_for_2alg['unrs'][i]['num']=data_in_full_noKV_nums[data_in_for_2alg['unrs'][i]['id']]

            noKV_pairs=make_noKV_pairs(data_in_for_2alg['unrs'],noKV_fact,ROOT)
            #правильные номера плавок
            #копия list_for_nums_adapt для того чтобы считать сколько плавок нужно поправить
            list_for_nums_adapt_copy=copy.deepcopy(list_for_nums_adapt)
            for i in solution['solution']:
                if (i['id_series']) in list_for_nums_adapt[0]:
                    i['num']+=(list_for_nums_adapt[1][list_for_nums_adapt[0].index(i['id_series'])])
                    list_for_nums_adapt[2][list_for_nums_adapt[0].index(i['id_series'])]-=1
                    if list_for_nums_adapt[2][list_for_nums_adapt[0].index(i['id_series'])]==0:
                        list_for_nums_adapt[0][list_for_nums_adapt[0].index(i['id_series'])]=-10

            data_out['solution_future']['recalc']=make_uns_from_indexes(solution['solution'])
            date_task['unrs']=copy.deepcopy(data_in_for_2alg['unrs'])
            summ=solution['summ']
            data_out['summ']=summ
            data_out['code']="200"
            solution2=[]

            with open(ROOT+"/statuses/"+status_file, "r") as read_file:
                status_data = json.load(read_file)
                status_data['message']='Поиск маршрутов для впо'
                status_data['code']=40
                with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                    json.dump(status_data,write_file,ensure_ascii=False)


            try:
                out_for_2alg=copy.deepcopy(data_out)
                out_for_2alg['data_in_full']=data_in_for_2alg
                solution2,vpo_message=planner_2(ROOT,out_for_2alg,noKV_pairs)
            except:
                solution2=[]
            '''
            out_for_2alg=copy.deepcopy(data_out)
            out_for_2alg['data_in_full']=data_in_for_2alg
            solution2,vpo_message=planner_2(ROOT,out_for_2alg,noKV_pairs)
            '''




            if solution2==[]:
                data_out['code']="300"

            data_out['solution_operational']['recalc']=solution2


            #расчетные выдержки и планы


            col_steel=[[],[],[],[],[]]
            for i in data['unrs']:
                col_steel[i['un']].append(i['num'])

            data_out['data_in_full']['unrs']=date_task['unrs']
            data_out['data_in_full']['TO']=date_task['to']

            data_out['data_in_full']['unrs']=adapt_max_vyd(data_in_def['unrs'],data_out['data_in_full']['unrs'])

            data_out['data_in_full'].update(calc_plan=calc_plans(data_out['data_in_full'],fact_plavs,data_out['solution_future']['recalc'],blow,datetime.datetime.fromtimestamp(day1_timestamp).date(),datetime.datetime.fromtimestamp(day2_timestamp).date()))

            data_out['data_in_full'].update(Smax_udch=count_Smax_udch(data_out['data_in_full'],fact_plavs,data_out['solution_future']['recalc'],blow,datetime.datetime.fromtimestamp(day1_timestamp).date(),datetime.datetime.fromtimestamp(day2_timestamp).date()))


            for i in range(len(data_out['data_in_full']['unrs'])):
                data_out['data_in_full']['unrs'][i].update(made_plavs=input_data['unrs'][i]['num']-data_out['data_in_full']['unrs'][i]['num'])


            for i in data_out['solution_future']['recalc']:
                i.update(shift=define_shift(i['start_on_kv'],day1_timestamp,day2_timestamp))


            add_plav_ids_to_plan(data_out['solution_future']['recalc'])
            data_out['solution_future']['recalc']=add_num_melt(fact_for_to,data_out['solution_future']['recalc'])

            if data_out['solution_operational']['recalc']!=[]:
                data_out=add_indexes(data_out,'recalc')

            data_out=add_flags(data_out,'recalc')

            #Изменение названий агрегатов на их индексы
            with open(ROOT+"/ag_ids.json", "r") as read_file:
                ags_ids=json.load(read_file)
            data_out=rename_agrs(data_out,ags_ids,'recalc')

            data_out=add_bigger_right_bounds_flags(data_in_def['unrs'],data_out,bigger_right_bounds,'recalc')

        if data_out['code']=="200":
################## или vpo_transportation_cons: false
            if vpo_message['to']==False or vpo_message['delta']==False or vpo_message['route']==False or vpo_message['fact']==False or vpo_message['transportation']==False:
                data_out['code']="500"
                data_out['vpo_message']=vpo_message

        #Возвращение num у noKV серий
        if len(data_in_full_noKV_nums.keys())!=0:
            for i in range(len(data_out['data_in_full']['unrs'])):
                if data_out['data_in_full']['unrs'][i]['id'] in data_in_full_noKV_nums.keys() and data_out['data_in_full']['unrs'][i]['num']==0:
                    data_out['data_in_full']['unrs'][i]['num']=data_in_full_noKV_nums[data_out['data_in_full']['unrs'][i]['id']]



        #сохрарения файлов логов
        end_planner_replanning()



    else:

        #Обнуление noKV серий
        data_in_full_noKV_nums=dict()
        for i in range(len(data['unrs'])):
            if data['unrs'][i].get('noKV')==False or data['unrs'][i].get('show_row')==False:
                data_in_full_noKV_nums[data['unrs'][i]['id']]=data['unrs'][i]['num']
                data['unrs'][i]['num']=0


        #чтение индексов для изменения номеров плавок в дальнейшем

        ids_ind_to_adapt=dict()
        for i in data['unrs']:
            ids_ind_to_adapt[i['id']]=i['index']-1


        starts_for_dates=[]
        for i in data['unrs']:
            starts_for_dates.append(i['start'])

        if 'date_start' in data.keys():
            '''
            day1_timestamp=int(mktime((datetime.datetime.strptime(data['date_start'].replace('.000Z',''), '%Y-%m-%dT%H:%M:%S')+timedelta(minutes=180)).timetuple()))
            day2_timestamp=day1_timestamp+86400
            '''

            try:
                day1_timestamp=int(float(data['date_start']))+3*60*60
                day2_timestamp=day1_timestamp+86400
            except:
                day1_timestamp=int(mktime(datetime.datetime.strptime(data['date_start'].split('T')[0], '%Y-%m-%d').timetuple()))
                day1_timestamp+=int(data['date_start'].split('T')[1].split(':')[0])*60*60+int(data['date_start'].split('T')[1].split(':')[1])+3*60*60
                day2_timestamp=day1_timestamp+86400

        else:
            day1_timestamp=min(starts_for_dates)
            day2_timestamp=day1_timestamp+86400
        starts_for_dates=None


        with open(ROOT+"/statuses/"+status_file, "r") as read_file:
            status_data = json.load(read_file)
            status_data['message']='Поиск решения'
            status_data['code']=10
            with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                json.dump(status_data,write_file,ensure_ascii=False)

        try:
            status,solution,date_task=planner_1(data,ROOT,day1_timestamp,day2_timestamp,list_un_kv_time)
        except:
            status=0
            date_task=dict(unrs=[],TO=[])
            solution=dict()
            solution['solution']=[]
            solution['code']="400"
            solution['summ']=0


        #для адаптации максимальных выдержек после всех манипуляций
        data_in_def=copy.deepcopy(date_task)

        bigger_right_bounds=False

        #циклическое увеличение правых границ в полтора раза
        if status==-1:
            bigger_right_bounds=True
            data.update(unrs=date_task['unrs'])
            out_with_message=dict(data_in_full=[],solution_future=dict(plan=[],recalc=[]),summ=0,code="100",message="Правые границы выдержек увеличены",new_to=[],solution_operational=dict(plan=[],recalc=[]))
            for i in range(1):
                with open(ROOT+"/statuses/"+status_file, "r") as read_file:
                    status_data = json.load(read_file)
                    status_data['message']='Поиск решения с увеличенными правыми границами'
                    status_data['code']=20
                    with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                        json.dump(status_data,write_file,ensure_ascii=False)
                for j in range(len(data['unrs'])):
                    if date_task['unrs'][j].get('fake_stan')!=True:
                        data['unrs'][j].update(max_vyd=date_task['unrs'][j]['max_vyd']*1.5,min_vyd=date_task['unrs'][j]['min_vyd'])
                if data['unrs']!=input_data['unrs']:
                    status, solution,date_task=planner_1(data,ROOT,day1_timestamp,day2_timestamp,list_un_kv_time)
                if status==1:
                    break


        data_out['data_in_full']=date_task
        data_out['code']=solution['code']
        data_out['summ']=solution['summ']


        for i in solution['solution']:
            i['num']+=ids_ind_to_adapt[i['id_series']]


        data_out['solution_future']['plan']=make_uns_from_indexes(solution['solution'])
        data_out['data_in_full']['TO']=data['to']
        data_out['data_in_full']['unrs']=adapt_max_vyd(data_in_def['unrs'],data_out['data_in_full']['unrs'])
        data_out['data_in_full'].update(calc_plan=calc_plans(data_out['data_in_full'],[],data_out['solution_future']['plan'],blow,datetime.datetime.fromtimestamp(day1_timestamp).date(),datetime.datetime.fromtimestamp(day2_timestamp).date()))
        data_out['data_in_full'].update(Smax_udch=count_Smax_udch(data_out['data_in_full'],[],data_out['solution_future']['plan'],blow,datetime.datetime.fromtimestamp(day1_timestamp).date(),datetime.datetime.fromtimestamp(day2_timestamp).date()))
        data_out['new_to']=[]
        for i in data_out['solution_future']['plan']:
            i.update(shift=define_shift(i['start_on_kv'],day1_timestamp,day2_timestamp))

        if status==1:
            if 'new_to' in solution.keys():
                data_out['new_to']=solution['new_to']
            else:
                data_out['new_to']=[]

            add_plav_ids_to_plan(data_out['solution_future']['plan'])
            data_out['solution_future']['plan']=add_num_melt([],data_out['solution_future']['plan'])

            #Поиск маршрутов ВПО

            with open(ROOT+"/statuses/"+status_file, "r") as read_file:
                status_data = json.load(read_file)
                status_data['message']='Поиск маршрутов для впо'
                status_data['code']=40
                with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                    json.dump(status_data,write_file,ensure_ascii=False)

            if data_out['solution_future']['plan']!=[]:
                try:
                    data_out['solution_operational']['plan'],vpo_message=planner_2(ROOT,data_out,[])
                    if data_out['solution_operational']['plan']==[]:
                        data_out['code']="300"
                except:
                    data_out['solution_operational']['plan']=[]
                    data_out['code']="400"

            else:
                data_out['solution_operational']['plan']=[]

            if data_out['solution_operational']['plan']!=[]:
                data_out=add_indexes(data_out,'plan')


            data_out=add_flags(data_out,'plan')

            #Изменение названий агрегатов на их индексы
            with open(ROOT+"/ag_ids.json", "r") as read_file:
                ags_ids=json.load(read_file)
            data_out=rename_agrs(data_out,ags_ids,'plan')

            data_out=add_bigger_right_bounds_flags(data_in_def['unrs'],data_out,bigger_right_bounds,'plan')

        data_out['task_name']=input_data.get('task_name')

        if data_out['code']=="200":
            if vpo_message['to']==False or vpo_message['delta']==False or vpo_message['route']==False or vpo_message['fact']==False or vpo_message['transportation']==False:
                data_out['code']="500"
                data_out['vpo_message']=vpo_message

        #Возвращение num у noKV серий
        if len(data_in_full_noKV_nums.keys())!=0:
            for i in range(len(data_out['data_in_full']['unrs'])):
                if data_out['data_in_full']['unrs'][i]['id'] in data_in_full_noKV_nums.keys() and data_out['data_in_full']['unrs'][i]['num']==0:
                    data_out['data_in_full']['unrs'][i]['num']=data_in_full_noKV_nums[data_out['data_in_full']['unrs'][i]['id']]


        with open(ROOT+"/data_out.json", "w") as write_file:
            json.dump(data_out,write_file,ensure_ascii=False)

        data_out['input_data']=input_data_def


        #сохрарения файлов логов
        if ip==None:
            ip='def_ip'
        if log_file==None:
            log_file='def_name'

        with open(ROOT+"/logs"+"/logs.txt", "a") as write_file:
            log=dict(ip=ip,task_name=log_file,time=time_str,replanning=False)
            write_file.write("\n"+str(log))
        with open(ROOT+"/logs"+"/"+log_file+"_time:"+time_str+".json", "w") as write_file:
            data_out['name']=log_file+"_time:"+time_str+".json"
            json.dump(data_out,write_file,ensure_ascii=False)


        with open(ROOT+"/statuses/"+status_file, "r") as read_file:
            status_data = json.load(read_file)
            status_data['stop']=2
            if data_out.get('code')=='400':
                status_data['message']='Ошибка ввода'
            else:
                if len(data_out['solution_future']['plan'])!=0:
                    if len(data_out['solution_operational']['plan'])!=0:
                        status_data['message']='Решение есть'
                        if data_out.get('code')=='500':
                            status_data['message']='Решения для ВПО найдено с нарушениями ограничений'
                    else:
                        status_data['message']='Решение для ВПО не найдено -- попробуйте увеличить нижнюю границу выдержек'
                else:
                    status_data['message']='Решение не найдено -- попробуйте скорректировать начало серий'
            with open(ROOT+"/statuses/"+status_file, "w") as write_file:
                json.dump(status_data,write_file,ensure_ascii=False)

        print(log_file+"_time:"+time_str+".json")


def check_status(proc,file):
    from time import sleep
    ROOT = os.path.dirname(os.path.abspath(__file__))
    change_time=0
    while 1:
        if os.stat(ROOT+"/statuses/"+file).st_mtime>change_time:
            change_time=os.stat(ROOT+"/statuses/"+file).st_mtime
            with open(ROOT+"/statuses/"+file, "r") as read_file:
                status = json.load(read_file)
            if status.get('stop')==1:
                out=dict(data_in_full=dict(unrs=[],TO=[]),solution_future=dict(plan=[],recalc=[]),summ=0,code="600",new_to=[],solution_operational=dict(plan=[],recalc=[]))   #завершение по кнопке
                #print(out)
                print("stop")
                proc.terminate()
                break

            #это чтобы отличать прерывание с веба от прерывания в конце расчета
            if status.get('stop')==2 or 'stopped' in str(proc):
                proc.terminate()
                break
        sleep(1)

    #удаление файла статуса (если он нужен будет после расчетов вебу то лучше это им и удалять)
    #path = os.path.join(os.path.abspath(os.path.dirname(__file__)), file)
    #os.remove(path)



    sys.exit()


if __name__ == '__main__':
    input_data = json.loads(sys.argv[1])

    ROOT = os.path.dirname(os.path.abspath(__file__))

##    rm_garbage(ROOT)

##    update_files(ROOT)


    with open(ROOT+'/input.json', mode='w', encoding='utf-8') as results_file:
        json.dump(input_data, results_file,ensure_ascii=False)

    if input_data.get('status_file')!=None and input_data.get('status_file')!="":
        status_file=input_data.get('status_file')
        status=dict(message="",stop=0,without_plan=0,bigger_left_bound=0,code=0)       #пока статус как для статус деф чтобы не ждать ответы
    else:
        status_file="status_def.json"
        input_data['status_file']=status_file
        status=dict(message="",stop=0,without_plan=0,bigger_left_bound=0,code=0)

    if input_data.get('task_name')==None or input_data.get('task_name')=='':
        input_data['task_name']='unknown_task'
    if input_data.get('ip')==None or input_data.get('ip')=='':
        input_data['ip']='unknown_ip'

    with open(ROOT+"/statuses/"+status_file, "w") as write_file:
        json.dump(status,write_file,ensure_ascii=False)


    procs = []

    proc = Process(target=planner_1_2,args=(input_data,))
    procs.append(proc)
    proc.start()

    proc = Process(target=check_status(procs[0],status_file))
    procs.append(proc)
    proc.start()


    for proc in procs:
        proc.join()


    #planner_1_2(input_data)
