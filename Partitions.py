import pandas as pd
import numpy as np
import random
import rados
import sys
import time

table_shape = [1000000, 1000]

def _complete(completion, data_read):
    pass


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


def row_workload_rowlayout(partition_shape, data_pool, percentage = 10):

    total_rows = table_shape[0]
    total_cols = table_shape[1]
    rand_rows = random.sample(range(total_rows), int(total_rows*percentage/100))
    
    obj_ids = []
    offsets = []
    lengths = []

    for row in rand_rows:
        obj_num_start = int(row/partition_shape[0]) * int(total_cols/partition_shape[1])
        obj_num_end = int(row/partition_shape[0] + 1) * int(total_cols/partition_shape[1])
        for i in range(obj_num_start, obj_num_end):
            offset_t = (partition_shape[1]*8)*(row%partition_shape[0])
            length_t = 8 * partition_shape[1]
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


def row_workload_collayout(partition_shape, data_pool, percentage = 10):

    total_rows = 1000000
    total_cols = 1000
    rand_rows = random.sample(range(total_rows), int(total_rows*percentage/100))
    
    obj_ids = []
    offsets = []
    lengths = []

    for row in rand_rows:

        obj_num_start = int(row/partition_shape[0]) * int(total_cols/partition_shape[1])
        obj_num_end = int(row/partition_shape[0] + 1) * int(total_cols/partition_shape[1])
        
        for i in range(obj_num_start, obj_num_end):
            for j in range(partition_shape[1]):
                offset_t = 8*(row%partition_shape[0]) + j*partition_shape[0]*8
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


def col_workload_collayout(partition_shape, data_pool, percentage = 10):
    
    total_rows = 1000000
    total_cols = 1000
    rand_cols = random.sample(range(total_cols), int(total_cols*percentage/100))
    
    obj_ids = []
    offsets = []
    lengths = []

    for col in rand_cols:

        obj_num_start = int(col/partition_shape[1])

        for i in range(int(total_rows/partition_shape[0])):
            obj_num = obj_num_start + i * (total_cols/partition_shape[1])
            offset_t = 8*partition_shape[0]*(col%partition_shape[1])
            length_t = 8*partition_shape[0]
            obj_ids.append(obj_num)
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


def col_workload_rowlayout(partition_shape, data_pool, percentage = 10):

    total_rows = 1000000
    total_cols = 1000
    rand_cols = random.sample(range(total_cols), int(total_cols*percentage/100))
    
    obj_ids = []
    offsets = []
    lengths = []

    for col in rand_cols:
        obj_num_start = int(col/partition_shape[1])
        for i in range(int(total_rows/partition_shape[0])):
            obj_num = obj_num_start + i * (total_cols/partition_shape[1])
            for j in range(partition_shape[0]):
                offset_t = 8 * (col%partition_shape[1]) + j*partition_shape[1]*8
                length_t = 8
                obj_ids.append(obj_num)
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


percentage = 1
datapool = 'datapool2'
partition_shapes = [[1000,1000], [2000,500],[4000,250], [5000,200], [8000,125], [10000,100], [20000, 50], [40000, 25],[50000, 20], [100000, 10], [200000, 5], [1000000, 1]]
row_row = []
row_col = []
col_row = []
col_col = []

data_file = open('results','a')

for partition_shape in partition_shapes:
    secs = row_workload_rowlayout(partition_shape , datapool, percentage)
    bandwidth = 1000000000 * 8 * percentage / 100 / secs / 1024
    row_row.append(bandwidth)
    data_file.write(str(bandwidth)+',')
    data_file.flush()

    secs = row_workload_collayout(partition_shape , datapool, percentage)
    bandwidth = 1000000000 * 8 * percentage / 100 / secs / 1024
    row_col.append(bandwidth)
    data_file.write(str(bandwidth)+',')
    data_file.flush()

    secs = col_workload_rowlayout(partition_shape , datapool, percentage)
    bandwidth = 1000000000 * 8 * percentage / 100 / secs / 1024
    col_row.append(bandwidth)
    data_file.write(str(bandwidth)+',')
    data_file.flush()

    secs = col_workload_collayout(partition_shape , datapool, percentage)
    bandwidth = 1000000000 * 8 * percentage / 100 / secs / 1024
    col_col.append(bandwidth)
    data_file.write(str(bandwidth)+'\n')
    data_file.flush()

data_file.close()