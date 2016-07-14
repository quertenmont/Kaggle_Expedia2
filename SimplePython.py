import datetime
import sys
from heapq import nlargest
from operator import itemgetter

popular_hotel_cluster = dict()



def fillme (s,best_s,hotel_cluster,append,checkfull):
    allfull=True
    if checkfull:
        for entry in s:
            allfull=allfull and (entry != '')
    if allfull==True:
        if  s in best_s:                               
            if hotel_cluster in best_s[s]:
                best_s[s][hotel_cluster] += append
            else:
                best_s[s][hotel_cluster] = append
        else:
            best_s[s] = dict()
            best_s[s][hotel_cluster] = append

def sortingFunc(a, b):
    if a[1] == b[1]:
        return (popular_hotel_cluster[ a[0] ] < popular_hotel_cluster[ b[0] ])
    else:
        return a[1] < b[1]

def resolveDraws(list):
   return sorted(list, sortingFunc)
   


def prepare_arrays_match(file_train):
    global popular_hotel_cluster

    file_train.readline()
    
    best_hotels_od_ulc = dict()
    best_hotels_uid_miss = dict()
    best_hotels_search_dest = dict()
    best_hotels_country = dict()
    best_s00 = dict()
    best_s01 = dict()
    total = 0

    # Calc counts
    while 1:
        line = file_train.readline().strip()
        total += 1

        if total % 1000000 == 0:
            print('Counting %i lines...' % total)

        if line == '':
            break
        
        arr = line.split(",")
        book_year = int(arr[0][:4])
        book_month = int(arr[0][5:7])
#       book_day = int(arr[0][8:10])
        user_location_city = arr[5]
        orig_destination_distance = arr[6]
        user_id = arr[7]
        srch_destination_id = arr[16]
        hotel_country = arr[21]
        hotel_market = arr[22]
        is_booking = float(arr[18])
        hotel_cluster = arr[23]

        append_0 = ((book_year - 2012)*12 + (book_month))
        append_1 = append_0 * append_0 *append_0 * (6.0 + 19.0*is_booking)
        append_2 = 2.0 + 8.0*is_booking

        #4
        s1 = (user_location_city, orig_destination_distance)
        fillme(s1,best_hotels_od_ulc,hotel_cluster,append_0,True)

        
        #1
        if orig_destination_distance != '' and is_booking==1:
            s00 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
            fillme(s00,best_s00,hotel_cluster,append_0,True)

        #2 leak
        if user_location_city != '' and orig_destination_distance != '' and user_id !='' and srch_destination_id != '' and is_booking==1:
            s01 = (user_id, srch_destination_id, hotel_country, hotel_market)
            fillme(s01,best_s01,hotel_cluster,append_0,False)

        #3 when the distance is missing, match by the others
        if user_location_city != '' and orig_destination_distance == '' and user_id !='' and srch_destination_id != '' and hotel_country != '' and is_booking==1:
            s0 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
            fillme(s0,best_hotels_uid_miss,hotel_cluster,append_0,False)


        #5
        s2 = (srch_destination_id,hotel_country,hotel_market)
        fillme(s2,best_hotels_search_dest,hotel_cluster,append_1,True)

        #5bis
        s2b = (srch_destination_id,-1, -1)
        fillme(s2b,best_hotels_search_dest,hotel_cluster,append_1,True)

        
        #6 most popular in the country of the hotel
        s3 = (hotel_country)
        fillme(s3,best_hotels_country,hotel_cluster,append_2,True)

        #7
        if hotel_cluster in popular_hotel_cluster:
            popular_hotel_cluster[hotel_cluster] += append_0
        else:
            popular_hotel_cluster[hotel_cluster] = append_0

    file_train.close()
    return best_s00,best_s01, best_hotels_country, best_hotels_od_ulc, best_hotels_uid_miss, best_hotels_search_dest


def gen_submission(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss,file_test):
    global popular_hotel_cluster

    file_test.seek(0, 0)
    now = datetime.datetime.now()
    path = 'submission_miloc.csv'
    out = open(path, "w")
    file_test.readline()
    total = 0
    total0 = 0
    total00 = 0
    total1 = 0
    total2 = 0
    total3 = 0
    total4 = 0
    out.write("id,hotel_cluster\n")
    top_world_clusters = nlargest(5, sorted(popular_hotel_cluster.items()), key=itemgetter(1))

    print top_world_clusters

    while 1:
        line = file_test.readline().strip()
        total += 1

        if total % 100000 == 0:
            print('Write %i lines...' % total)

        if line == '':
            break

        arr = line.split(",")
        id = arr[0]
        user_location_city = arr[6]
        orig_destination_distance = arr[7]
        user_id = arr[8]
        srch_destination_id = arr[17]
        hotel_country = arr[20]
        hotel_market = arr[21]

        out.write(str(id) + ',')
        filled = []

        s1 = (user_location_city, orig_destination_distance)
        if s1 in best_hotels_od_ulc:
            d = best_hotels_od_ulc[s1]
            topitems = resolveDraws(nlargest(5, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total1 += 1

        if orig_destination_distance == '':
            s0 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
            if s0 in best_hotels_uid_miss:
                d = best_hotels_uid_miss[s0]
                topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
                for i in range(len(topitems)):
                    if topitems[i][0] in filled:
                        continue
                    if len(filled) == 5:
                        break
                    out.write(' ' + topitems[i][0])
                    filled.append(topitems[i][0])
                    total0 += 1

        s00 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
        s01 = (user_id,                     srch_destination_id, hotel_country, hotel_market)

        if s01 in best_s01 and s00 not in best_s00:
            d = best_s01[s01]
            topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total00 += 1


        s2 = (                              srch_destination_id, hotel_country, hotel_market)
        if s2 in best_hotels_search_dest:
            d = best_hotels_search_dest[s2]
            topitems = resolveDraws(nlargest(5, d.items(), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total2 += 1


        s2b = (                              srch_destination_id, -1, -1)
        if s2b in best_hotels_search_dest:
            d = best_hotels_search_dest[s2b]
            topitems = resolveDraws(nlargest(5, d.items(), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total2 += 1


        s3 = (hotel_country)
        if s3 in best_hotels_country:
            d = best_hotels_country[s3]
            topitems = resolveDraws(nlargest(5, d.items(), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total3 += 1
                
        for i in range(len(top_world_clusters)):
            if top_world_clusters[i][0] in filled:
                continue
            if len(filled) == 5:
                break
            out.write(' ' + top_world_clusters[i][0])
            filled.append(top_world_clusters[i][0])
            total4 += 1

        out.write("\n")
    out.close()
    print('Total 1:  %i \n' % total1)
    print('Total 0:  %i \n' % total0)
    print('Total 00: %i \n' % total00)
    print('Total 2:  %i \n' % total2)
    print('Total 3:  %i \n' % total3)
    print('Total 4:  %i \n' % total4)



def gen_submissionA(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss, file_test):
    global popular_hotel_cluster
    file_test.seek(0, 0)
    now = datetime.datetime.now()
    path = 'submission_milocA.csv'
    out = open(path, "w")
    file_test.readline()
    total = 0
    total0 = 0
    total00 = 0
    total1 = 0
    total2 = 0
    total3 = 0
    total4 = 0
    out.write("id,hotel_cluster\n")
    top_world_clusters = nlargest(5, sorted(popular_hotel_cluster.items()), key=itemgetter(1))

    print top_world_clusters

    while 1:
        line = file_test.readline().strip()
        total += 1

        if total % 100000 == 0:
            print('Write %i lines...' % total)

        if line == '':
            break

        arr = line.split(",")
        id = arr[0]
        user_location_city = arr[6]
        orig_destination_distance = arr[7]
        user_id = arr[8]
        srch_destination_id = arr[17]
        hotel_country = arr[20]
        hotel_market = arr[21]

        out.write(str(id) + ',')
        filled = []

        s1 = (user_location_city, orig_destination_distance)
        if s1 in best_hotels_od_ulc:
            d = best_hotels_od_ulc[s1]
            topitems = resolveDraws(nlargest(5, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total1 += 1

        out.write("\n")
    out.close()


def gen_submissionB(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss ,file_test): 
    global popular_hotel_cluster
    file_test.seek(0, 0)
    now = datetime.datetime.now()
    path = 'submission_milocB.csv'
    out = open(path, "w")
    file_test.readline()
    total = 0
    total0 = 0
    total00 = 0
    total1 = 0
    total2 = 0
    total3 = 0
    total4 = 0
    out.write("id,hotel_cluster\n")
    top_world_clusters = nlargest(5, sorted(popular_hotel_cluster.items()), key=itemgetter(1))

    print top_world_clusters

    while 1:
        line = file_test.readline().strip()
        total += 1

        if total % 100000 == 0:
            print('Write %i lines...' % total)

        if line == '':
            break

        arr = line.split(",")
        id = arr[0]
        user_location_city = arr[6]
        orig_destination_distance = arr[7]
        user_id = arr[8]
        srch_destination_id = arr[17]
        hotel_country = arr[20]
        hotel_market = arr[21]

        out.write(str(id) + ',')
        filled = []

        s1 = (user_location_city, orig_destination_distance)
        if s1 in best_hotels_od_ulc:
            d = best_hotels_od_ulc[s1]
            topitems = resolveDraws(nlargest(5, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total1 += 1

        if orig_destination_distance == '':
            s0 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
            if s0 in best_hotels_uid_miss:
                d = best_hotels_uid_miss[s0]
                topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
                for i in range(len(topitems)):
                    if topitems[i][0] in filled:
                        continue
                    if len(filled) == 5:
                        break
                    out.write(' ' + topitems[i][0])
                    filled.append(topitems[i][0])
                    total0 += 1

        s00 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
        s01 = (user_id,                     srch_destination_id, hotel_country, hotel_market)

        if s01 in best_s01 and s00 not in best_s00:
            d = best_s01[s01]
            topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total00 += 1


        out.write("\n")
    out.close()
    print('Total 1:  %i \n' % total1)
    print('Total 0:  %i \n' % total0)
    print('Total 00: %i \n' % total00)
    print('Total 2:  %i \n' % total2)
    print('Total 3:  %i \n' % total3)
    print('Total 4:  %i \n' % total4)



def gen_submissionC(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss ,file_test):
    global popular_hotel_cluster
    file_test.seek(0, 0)
    now = datetime.datetime.now()
    path = 'submission_milocC.csv'
    out = open(path, "w")
    file_test.readline()
    total = 0
    total0 = 0
    total00 = 0
    total1 = 0
    total2 = 0
    total3 = 0
    total4 = 0
    out.write("id,hotel_cluster\n")
    top_world_clusters = nlargest(5, sorted(popular_hotel_cluster.items()), key=itemgetter(1))

    print top_world_clusters

    while 1:
        line = file_test.readline().strip()
        total += 1

        if total % 100000 == 0:
            print('Write %i lines...' % total)

        if line == '':
            break

        arr = line.split(",")
        id = arr[0]
        user_location_city = arr[6]
        orig_destination_distance = arr[7]
        user_id = arr[8]
        srch_destination_id = arr[17]
        hotel_country = arr[20]
        hotel_market = arr[21]

        out.write(str(id) + ',')
        filled = []

        s1 = (user_location_city, orig_destination_distance)
        if s1 in best_hotels_od_ulc:
            d = best_hotels_od_ulc[s1]
            topitems = resolveDraws(nlargest(5, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total1 += 1

        if orig_destination_distance == '':
            s0 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
            if s0 in best_hotels_uid_miss:
                d = best_hotels_uid_miss[s0]
                topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
                for i in range(len(topitems)):
                    if topitems[i][0] in filled:
                        continue
                    if len(filled) == 5:
                        break
                    out.write(' ' + topitems[i][0])
                    filled.append(topitems[i][0])
                    total0 += 1

        s00 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
        s01 = (user_id,                     srch_destination_id, hotel_country, hotel_market)

        if s01 in best_s01 and s00 not in best_s00:
            d = best_s01[s01]
            topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total00 += 1


        s2 = (                              srch_destination_id, hotel_country, hotel_market)
        if s2 in best_hotels_search_dest:
            d = best_hotels_search_dest[s2]
            topitems = resolveDraws(nlargest(5, d.items(), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total2 += 1

        out.write("\n")
    out.close()
    print('Total 1:  %i \n' % total1)
    print('Total 0:  %i \n' % total0)
    print('Total 00: %i \n' % total00)
    print('Total 2:  %i \n' % total2)
    print('Total 3:  %i \n' % total3)
    print('Total 4:  %i \n' % total4)



def gen_submissionD(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss ,file_test):
    global popular_hotel_cluster
    file_test.seek(0, 0)
    now = datetime.datetime.now()
    path = 'submission_milocD.csv'
    out = open(path, "w")
    file_test.readline()
    total = 0
    total0 = 0
    total00 = 0
    total1 = 0
    total2 = 0
    total3 = 0
    total4 = 0
    out.write("id,hotel_cluster\n")
    top_world_clusters = nlargest(5, sorted(popular_hotel_cluster.items()), key=itemgetter(1))

    print top_world_clusters

    while 1:
        line = file_test.readline().strip()
        total += 1

        if total % 100000 == 0:
            print('Write %i lines...' % total)

        if line == '':
            break

        arr = line.split(",")
        id = arr[0]
        user_location_city = arr[6]
        orig_destination_distance = arr[7]
        user_id = arr[8]
        srch_destination_id = arr[17]
        hotel_country = arr[20]
        hotel_market = arr[21]

        out.write(str(id) + ',')
        filled = []

        s1 = (user_location_city, orig_destination_distance)
        if s1 in best_hotels_od_ulc:
            d = best_hotels_od_ulc[s1]
            topitems = resolveDraws(nlargest(5, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total1 += 1

        if orig_destination_distance == '':
            s0 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
            if s0 in best_hotels_uid_miss:
                d = best_hotels_uid_miss[s0]
                topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
                for i in range(len(topitems)):
                    if topitems[i][0] in filled:
                        continue
                    if len(filled) == 5:
                        break
                    out.write(' ' + topitems[i][0])
                    filled.append(topitems[i][0])
                    total0 += 1

        s00 = (user_id, user_location_city, srch_destination_id, hotel_country, hotel_market)
        s01 = (user_id,                     srch_destination_id, hotel_country, hotel_market)

        if s01 in best_s01 and s00 not in best_s00:
            d = best_s01[s01]
            topitems = resolveDraws(nlargest(4, sorted(d.items()), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total00 += 1


        s2 = (                              srch_destination_id, hotel_country, hotel_market)
        if s2 in best_hotels_search_dest:
            d = best_hotels_search_dest[s2]
            topitems = resolveDraws(nlargest(5, d.items(), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total2 += 1


        s2b = (                              srch_destination_id, -1, -1)
        if s2b in best_hotels_search_dest:
            d = best_hotels_search_dest[s2b]
            topitems = resolveDraws(nlargest(5, d.items(), key=itemgetter(1)) )
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total2 += 1


        out.write("\n")
    out.close()
    print('Total 1:  %i \n' % total1)
    print('Total 0:  %i \n' % total0)
    print('Total 00: %i \n' % total00)
    print('Total 2:  %i \n' % total2)
    print('Total 3:  %i \n' % total3)
    print('Total 4:  %i \n' % total4)




#parameters to run

measure_score = True

if measure_score:
    file_train = open("../../../check_score/train20132014I.csv", "r")
    file_test = open("../../../check_score/test_2014II_isbooking.csv", "r")

else:
    file_train = open("../input/train.csv", "r")
    file_test = open("../input/test.csv", "r")

best_s00,best_s01,best_hotels_country, best_hotels_od_ulc, best_hotels_uid_miss, best_hotels_search_dest = prepare_arrays_match(file_train)
#gen_submissionA(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss,file_test)
#gen_submissionB(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss,file_test)
#gen_submissionC(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss,file_test) 
#gen_submissionD(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss,file_test)

gen_submission(best_s00, best_s01,best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss,file_test)

for i in range(0,99):
   print("HOTEL %d = %f" % (i, popular_hotel_cluster[str(i)] ) )

