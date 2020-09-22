import pandas as pd
import numpy as np
import random
import rados
import sys
import time


# convert the df to byte stream
def df_to_bytes(df):
    bts = bytes(0)
    rows = df.shape[0]
    cols = df.shape[1]

    for i in range(cols):
        for j in range(rows):
            bts = bts + df[i][j].tobytes()
    return bts


# get the df given the shape
def get_df(rows_num,cols_num):
    cols = range(cols_num)
    df = pd.DataFrame(np.random.randint(0,1000000000,size= (rows_num,cols_num)), columns=cols)
    return df


def _complete(completion, data_read):
    pass


def inject_data(partition_shape, data_pool):
    df = get_df(partition_shape[0], partition_shape[1])
    bts = df_to_bytes(df)
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(data_pool)
    for i in range(int(1000/partition_shape[1]+1) * int(1000000/partition_shape[0] + 1)):
        comp = ioctx.aio_write_full(str(i),bts,oncomplete=_complete)
    comp.wait_for_complete()
    time.sleep(30)
    ioctx.close()
    cluster.shutdown()


def merge_read_ops(obj_ids, offsets, lengths):

    df_data = {"obj_ids" : obj_ids, "offsets" : offsets, "lengths" : lengths}
    df = pd.DataFrame(df_data)
    df = df.sort_values(['obj_ids', 'offsets'], ascending=[True, True], ignore_index = True)

    obj_ids_new = []
    offsets_new = []
    lengths_new = []

    j = 0
    obj_ids_new.append(int(df['obj_ids'][0]))
    offsets_new.append(int(df['offsets'][0]))
    lengths_new.append(int(df['lengths'][0]))

    for i in range(1, len(obj_ids)):
        if (int(df['obj_ids'][i]) == obj_ids_new[j]) and (int(df['offsets'][i]) == offsets_new[j] + lengths_new[j]):
            lengths_new[j] = lengths_new[j] + int(df['lengths'][i])
        else:
            obj_ids_new.append(int(df['obj_ids'][i]))
            offsets_new.append(int(df['offsets'][i]))
            lengths_new.append(int(df['lengths'][i]))
            j = j+1

    return obj_ids_new, offsets_new, lengths_new


def row_workload_collayout(data_pool, percentage = 10):
    start = time.time()
    rows = random.sample(range(1000000), 1000000*percentage/100)
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(data_pool)
    comp = None
    for i in range(1000):
        for row in rows:
            comp = ioctx.aio_read(str(i), offset=row*8, length=8, oncomplete=_complete)
    comp.wait_for_complete()
    ioctx.close()
    cluster.shutdown()
    stop = time.time()
    return stop - start

def col_workload_collayout(data_pool, percentage = 10):
    start = time.time()
    cols = random.sample(range(1000), 1000*percentage/100)
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(data_pool)
    comp = None
    for col in cols:
        comp = ioctx.aio_read(str(col), offset=0, length=8000000,  oncomplete=_complete)
    comp.wait_for_complete()
    ioctx.close()
    cluster.shutdown()
    stop = time.time()
    return stop - start 


def row_worload_rowlayout(data_pool, percentage = 10):

    
    total_rows = 1000000
    rand_rows = random.sample(range(1000000), int(total_rows*percentage/100))
    
    obj_ids = []
    offsets = []
    lengths = []

    for row in rand_rows:
        obj_num_start = int(row/1000)
        offset_t = (1000*8)*(row%1000)
        length_t = 8 * 1000
        obj_ids.append(obj_num_start)
        offsets.append(offset_t)
        lengths.append(length_t)
    
    obj_ids, offsets, lengths = merge_read_ops(obj_ids, offsets, lengths)

    print('Total requests count after merging:' + str(len(obj_ids)))

    start = time.time()
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(data_pool)

    comp = None
    for i in range(len(obj_ids)):
        obj_id = obj_ids[i]
        offset = offsets[i]
        length = lengths[i]
        comp = ioctx.aio_read(str(obj_id), offset=offset, length=length,  oncomplete=_complete)
    comp.wait_for_complete()

    ioctx.close()
    cluster.shutdown()

    stop = time.time()
    return (stop - start)


def col_worload_rowlayout(data_pool, percentage = 10):
    
    total_cols = 1000
    rand_cols = random.sample(range(1000), int(total_cols*percentage/100))
    row_groups_num = int(1000000/1000)
    
    obj_ids = []
    offsets = []
    lengths = []

    for col in rand_cols:
        for i in range(row_groups_num):
            for j in range(1000):
                offset_t = 8 *col + j*8
                length_t = 8
                obj_ids.append(i)
                offsets.append(offset_t)
                lengths.append(length_t)
    
    obj_ids, offsets, lengths = merge_read_ops(obj_ids, offsets, lengths)

    print('Total requests count after merging:' + str(len(obj_ids)))

    start = time.time()

    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx(data_pool)

    comp = None
    for i in range(len(obj_ids)):
        obj_id = obj_ids[i]
        offset = offsets[i]
        length = lengths[i]
        comp = ioctx.aio_read(str(obj_id), offset=offset, length=length,  oncomplete=_complete)    
    
    comp.wait_for_complete()
    ioctx.close()
    cluster.shutdown()
    stop = time.time()
    return (stop - start)
            
# one time efferts. 1000*1000*8 bytes which is about 8MB objects
inject_data([1000,1000], 'datapool2')           

# total table size is 8000MB, 1% is 80MB
secs = row_worload_rowlayout('datapool2', 1)
print('row worload row laytout:' + str(secs))
time.sleep(15)
secs = col_worload_rowlayout('datapool2', 1)
print('col worload row laytout:' + str(secs))
time.sleep(15)
secs = row_workload_collayout('datapool2', 1)
print('row worload col laytout:' + str(secs))
time.sleep(15)
secs = col_workload_collayout('datapool2', 1)
print('col worload col laytout:' + str(secs))