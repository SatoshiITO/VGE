#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
# coding: UTF-8

import os,sys
import shutil
import pickle
import hashlib
import time

import subprocess

from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
#from restartconf import *
from VGE.get_vge_conf import get_vge_conf

chk_work_dir=""
chk_target_dir=""
chk_checkpoint_dir=""

def checkpoint_restart(target_dir):

    elapsed_time=time.time()
#    init_restart_conf()

    logger=getLogger(__name__)
    logger.setLevel(INFO)
#    verbose = int(get_restart_conf("verbose", 0))
    verbose = int(get_vge_conf("vge","verbose", 0))
    if verbose == 0:
        logger.setLevel(CRITICAL)
    elif verbose == 1 or verbose == 2:
        logger.setLevel(INFO)
    elif verbose == 3:
        logger.setLevel(DEBUG)
    basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')
    pname="Pipeline(CHECKPOINT RESTART)"
    process_id = os.getpid()
    logger.info("%s pid[%i]: checkpoint judgment and reading start task " %(pname,process_id))

#    chk_file = int(get_restart_conf("chk_file", 2))
    chk_file = int(get_vge_conf("restart","chk_file", 2))

    # common parameters
    global chk_work_dir
    global chk_target_dir
    global chk_checkpoint_dir

    work_dir_path = os.path.dirname(target_dir)
    work_dir_path = os.path.abspath(work_dir_path)
    work_dir_path = os.path.normpath(work_dir_path)
    checkpoint_dir_path = os.path.join(target_dir,"checkpoint")
    checkpoint_dir_path = os.path.abspath(checkpoint_dir_path)
    checkpoint_dir_path = os.path.normpath(checkpoint_dir_path)
    target_dir_path = os.path.abspath(target_dir)
    target_dir_path = os.path.normpath(target_dir_path)
    chk_work_dir = work_dir_path
    chk_target_dir = target_dir_path
    chk_checkpoint_dir = checkpoint_dir_path

    restartdata_dir = os.path.join(chk_checkpoint_dir,"backupdata")
    restartdata_dir = os.path.abspath(restartdata_dir)
    restartdata_dir = os.path.normpath(restartdata_dir)

    # reconst_checkpoint
    flag_skiptask  = {}
    flag_starttask = {}

    if not os.path.isdir(chk_checkpoint_dir):
        logger.info("%s pid[%i]: normal start" %(pname,process_id))
        elapsed_time=time.time() - elapsed_time
        logger.info("%s pid[%i]: checkpoint_restart elapsed_time %f[sec]" %(pname,process_id,elapsed_time))
        return flag_skiptask, flag_starttask, {}

    # checkpoint mstart file
    masterfile_list = []
    # チェックポイントマスタのリスト作成
    for filename in os.listdir(chk_checkpoint_dir):
        basename, ext = os.path.splitext(filename)
        if ".mst" == ext:
            masterfile_list.append(filename)

    # チェックポイントマスタファイルが１つも無ければ
    # リスタートしない
    if not masterfile_list:
        logger.info("%s pid[%i]: not restart, checkpoint start" %(pname,process_id))
        elapsed_time=time.time() - elapsed_time
        logger.info("%s pid[%i]: checkpoint_restart elapsed_time %f[sec]" %(pname,process_id,elapsed_time))
        return flag_skiptask, flag_starttask, {}

    set_task_list = []
    set_checkpointfiles = {}
    dict_route = {}
    for mstfile in masterfile_list:
        chk_masterfile = os.path.join(chk_checkpoint_dir,mstfile) 
        logger.debug("%s pid[%i]: checkpoint master file path: %s" %(pname,process_id,chk_masterfile))

        if os.path.isfile(chk_masterfile):
            try:
                checkpoint_master = open(chk_masterfile, "rb")
                logger.info("%s pid[%i]: restart start, open checkpoint master" %(pname,process_id))
                tmplist = checkpoint_master.readlines()
                checkpoint_master.close()
            except IOError as e:
                logger.info("%s pid[%i]: error was masterfile open [%s]. " %(pname,process_id,str(e)))
                exit()
        else:
            logger.info("%s pid[%i]: not restart, checkpoint start" %(pname,process_id))
            exit()

        # チェックポイントマスタデータの中身の確認
        # マスタデータの第1カラムがチェックポイントを作ったtask
        # 第2カラムがチェックポイントデータのパス
        # 第1カラムがfinalならば何もしない
        for line in tmplist:
            line_list = line.split(",")
            task = line_list[0]
            set_task_list.append(task)
            # 最初は長さが1の配列なので、このifが無いとelifでエラーが出る
            if (len(set_task_list) == 1):
                set_checkpointfiles[task] = [line_list[1].strip()]
            # 1つ前のTask名が同じなら真
            elif (set_task_list[-2] == set_task_list[-1]):
                set_checkpointfiles[task].append(line_list[1].strip())
            else:
                set_checkpointfiles[task] = [line_list[1].strip()]
        if set_task_list[-1] != "final":
            logger.debug("%s pid[%i]: set_task_list: %s" %(pname,process_id,set_task_list))
            flag_starttask[set_task_list[-1]] = True

            # タスク名とルートタグを紐づける辞書を作る
            # dict_route[task] = マスタファイルのルートタグの部分(basenameの0-10番目までが"checkpoint_")
            basename, ext = os.path.splitext(mstfile)
            dict_route[task] = basename[11:]

    # マスターファイルの情報を元に、flagを設定
    for task in set_task_list:
        # すでにチェックポイントが書かれていたらskip
        # checkpointtag = チェックポイントファイルのタスク名の部分(basenameの0-10番目までが"checkpoint_")
        for checkpointpath in set_checkpointfiles[task]:
            basename, ext = os.path.splitext(os.path.basename(checkpointpath))
            checkpointtag = basename[11:]
            flag_skiptask[checkpointtag] = True

    # read checkpointdata
    # verify_checkpoint
    restart_files_list_dict = {}
    for task, startflag in flag_starttask.items():
        new_outputfiles_list=[]

        for checkpointpath in set_checkpointfiles[task]: 
            # チェックポイントデータのパスの修正
            chkpoint_filename = os.path.basename(checkpointpath)
            checkpoint_absdir = os.path.abspath(chk_checkpoint_dir)
            checkpoint_absdir = os.path.normpath(checkpoint_absdir)
            new_checkpoint_filepath = os.path.join(checkpoint_absdir,chkpoint_filename)
            new_checkpoint_filepath = os.path.normpath(new_checkpoint_filepath)
            readfilename = new_checkpoint_filepath

            checkpointdata = None
            binalyfile=None

            # チェックポイントデータのopen
            try:
                binalyfile=open(readfilename, "rb")
                loaded_package = pickle.load(binalyfile)
                binalyfile.close()
                checkpointdata = pickle.loads(loaded_package)
            except IOError as e:
                exit()
            
            # ファイルの整合性チェック
            if chk_file == 0:
                pass

            elif chk_file == 1:
                restart_input_files_set = set()
                for root, dirs, files in os.walk(restartdata_dir + "_" +  dict_route[task]):
                    target_dirname = root[len(restartdata_dir + "_" +  dict_route[task])+1:]
                    #if target_dirname is not "":
                    if target_dirname is "":
                        target_dirname="."
                        
                        #for filename in files:
                        #    restart_input_files_set.add(target_dirname + "/" + filename)
                    for filename in files:
                        restart_input_files_set.add(target_dirname + "/" + filename)

                if not (set(checkpointdata["targetdirdict"]["filename_list"]) <= restart_input_files_set):
                    logger.warning("%s pid[%i]: (VERIFY WARNING) can not restart because file size mismatch...." %(pname,process_id))
                    logger.debug("%s pid[%i]: (VERIFY WARNING) restart_input_files_set: %s" %(pname,process_id,restart_input_files_set))
                    logger.debug("%s pid[%i]: (VERIFY WARNING) set(checkpointdata[\"targetdirdict\"][\"filename_list\"]): %s" %(pname,process_id,set(checkpointdata["targetdirdict"]["filename_list"])))
                    exit()

            elif chk_file == 2:
                for i in range(len(checkpointdata["targetdirdict"]["filename_list"])):
                    restart_data_file = os.path.join(restartdata_dir + "_" +  dict_route[task], checkpointdata["targetdirdict"]["filename_list"][i])
                    if os.path.islink(restart_data_file) and not os.path.isfile(restart_data_file):
                        # シンボリックリンクのファイルのリンクを修正する
                        current_dir = os.getcwd()
                        current_dir_len = len(current_dir.split("/"))
                        realpath = os.path.realpath(restart_data_file)
                        # カレントディレクトリ部分の名前を置き換える＠京向け
                        realpath_split=realpath.split("/")
                        dircount = 0
                        new_path = ""
                        for dirname in realpath_split:
                            dircount = dircount + 1
                            if dircount > current_dir_len:
                                new_path = new_path + "/" + dirname
                        new_src_file = os.path.normpath(current_dir+new_path)
                        os.unlink(restart_data_file)
                        os.symlink(new_src_file, restart_data_file)
                        logger.debug("%s pid[%i]: re symbolic link %s to %s" %(pname,process_id,restartdata_dir,new_src_file))
                    if not os.path.isfile(restart_data_file):
                        logger.warning("%s pid[%i]: (VERIFY WARNING) can not restart because 'restart_data_file' does not exist...." %(pname,process_id))
                        logger.debug("%s pid[%i]: (VERIFY WARNING) restart_data_file: %s" %(pname,process_id,restart_data_file))
                        exit()
                    if os.stat(restart_data_file).st_size != checkpointdata["targetdirdict"]["size_list"][i]:
                        logger.warning("%s pid[%i]: (VERIFY WARNING) can not restart because file size does not match" %(pname,process_id))
                        logger.debug("%s pid[%i]: (VERIFY WARNING) restart_data_file: %s" %(pname,process_id,restart_data_file))
                        exit()
        
            # 次のタスクの入力に渡すoutputfilesを作る(チェックポイントデータのパスを修正)
            ret=0
            chkpointd_outputfiles_list = checkpointdata["outputfiles"]
            chkpointd_target_dir = checkpointdata["targetdir"]
         
            # 前回実行時のアウトプットの各要素をみる。要素は、リストの場合(複数ファイル)と文字列の場合(ファイル単体)がある。
            for x in chkpointd_outputfiles_list:
                if type(x) is list:
                    new_outputfiles_list2=[]
                    # xの各要素が一つのファイルのパス。これらを新しいパスにしてリストに追加
                    for output_file_path in x:
                        new_file_name = os.path.join(chk_target_dir, output_file_path[len(chkpointd_target_dir)+1:])
                        new_file_name = os.path.abspath(new_file_name)
                        new_file_name = os.path.normpath(new_file_name)
                        new_outputfiles_list2.append(new_file_name)
                    new_outputfiles_list.append(new_outputfiles_list2)
            
                else:
                    # xは一つのファイルのパス。これを新しいパスにしてリストに追加
                    new_file_name = os.path.join(chk_target_dir, x[len(chkpointd_target_dir)+1:])
                    new_file_name = os.path.abspath(new_file_name)
                    new_file_name = os.path.normpath(new_file_name)
                    new_outputfiles_list.append(new_file_name)
            
            restart_files_list_dict[dict_route[task]] = new_outputfiles_list
            logger.debug("%s pid[%i]: new_outputfiles_list: %s" %(pname,process_id,new_outputfiles_list))
            logger.debug("%s pid[%i]: task: %s" %(pname,process_id, task))
            logger.debug("%s pid[%i]: dict_route: %s" %(pname,process_id, dict_route[task]))

        # リスタート用ファイルの再配置
        backup_dirs = os.listdir(restartdata_dir + "_" + dict_route[task])
        for backup_dir in backup_dirs:
            src_backup_dir = os.path.join(restartdata_dir + "_" + dict_route[task], backup_dir)
            args = ["cp", "-lr", src_backup_dir, target_dir]
            ret = subprocess.call(args)
            logger.warning("%s pid[%i]: (WRITE WARNING) restored backup file, subprocess.call retcode %d" \
                       %(pname,process_id,ret))
            logger.debug("%s pid[%i]: src_backup_dir: %s" %(pname,process_id,src_backup_dir))

    elapsed_time=time.time() - elapsed_time
    logger.info("%s pid[%i]: checkpoint_restart elapsed_time %f[sec]" %(pname,process_id,elapsed_time))

    return flag_skiptask, flag_starttask, restart_files_list_dict

# checkpoint_route_tag
# common or route_A or route_B
#   共通ルート or 変異ルート or SVルート
# checkpoint_tag_next_list,checkpoint_route_tag_next_list
#   チェックポイントを取ったタスクの次に実行するタスクとルート
#   通常は１要素、task5A後の分岐時に2要素
def write_checkpoint(checkpoint_tag, checkpoint_tag_next_list, checkpoint_route_tag_next_list, output_files):

   elapsed_time=time.time()

   logger=getLogger(__name__)
   logger.setLevel(INFO)
#   verbose = int(get_restart_conf("verbose", 0))
   verbose = int(get_vge_conf("vge","verbose", 0))
   if verbose == 0:
       logger.setLevel(CRITICAL)
   elif verbose == 1 or verbose == 2:
       logger.setLevel(INFO)
   elif verbose == 3:
       logger.setLevel(DEBUG)
   basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')

   pname="Pipeline(CHECKPOINT WRITE)"
   process_id = os.getpid()
   logger.info("%s pid[%i]: collecting files in target output directory...." %(pname,process_id))

   # チェックポイントを書き出さないタスクの処理
#   exclude_checkpoint_list = list(get_restart_conf("exclude_checkpoint_list", []))
   exclude_checkpoint_list = list(get_vge_conf("restart","exclude_checkpoint_list", []))

   for exclude_checkpoint_tag  in exclude_checkpoint_list:
       if checkpoint_tag ==  exclude_checkpoint_tag:
           logger.warning("%s pid[%i]: exclude task for %s" \
                          %(pname,process_id,exclude_checkpoint_tag))
           elapsed_time=time.time() - elapsed_time
           logger.info("%s pid[%i]: write_checkpoint elapsed_time %f[sec]" %(pname,process_id,elapsed_time))
           return 0

   work_dir = chk_work_dir
   target_dir = chk_target_dir
   checkpoint_dir = chk_checkpoint_dir
   
   # チェックポイントディレクトリの作成
   if not os.path.isdir(checkpoint_dir): os.mkdir(checkpoint_dir)
   
   target_list = []
   for route_tag_index in range(len(checkpoint_route_tag_next_list)):
       elapsed_time_mkchkpoint1=time.time()
    
       # set target directory
       target_absdir = os.path.abspath(target_dir)
       target_absdir = os.path.normpath(target_absdir)
       # make a target_dir_dict
       targetdir_dict={}
       targetdir_dict["filename_list"]=[]
       targetdir_dict["size_list"]=[]
       
       flag=0
       for root, dirs, files in os.walk(target_absdir):
           # checkpoint_dir は対象外
           #dirs[:] = [d for d in dirs if os.path.basename(checkpoint_dir) not in os.path.join(root, d)]
           dirs[:] = [d for d in dirs if os.path.abspath(checkpoint_dir) not in os.path.join(root, d)]
           if flag == 0:
              for d in dirs:
                  target_list.append(os.path.join(root,d))
              for filename in files:
                  target_list.append(os.path.join(root,filename))
              flag=1

           target_dirname = root[len(target_absdir)+1:]
           if target_dirname is "":
               target_dirname="."
           #    for filename in files:
           #        targetdir_dict["filename_list"].append(target_dirname + "/" + filename)
           #        targetdir_dict["size_list"].append(os.stat(root + "/" + filename).st_size) 
           for filename in files:
                if os.path.isfile(root + "/" + filename):
                    targetdir_dict["filename_list"].append(target_dirname + "/" + filename)
                    targetdir_dict["size_list"].append(os.stat(root + "/" + filename).st_size) 
      
       checkpointdata = {}
       checkpointdata["outputfiles"] = output_files
       checkpointdata["targetdir"] = target_dir
       checkpointdata["targetdirdict"] = targetdir_dict
    
       checkpoint_absdir = os.path.abspath(checkpoint_dir)
       checkpoint_absdir = os.path.normpath(checkpoint_absdir)
    
       # make restartdata filename
       temp_checkpoint_tag = checkpoint_tag.replace(" ","")
       checkpoint_filename = "checkpoint_" + temp_checkpoint_tag + ".chk"
       writefilenamepath = os.path.join(checkpoint_absdir,checkpoint_filename)
       writefilenamepath = os.path.normpath(writefilenamepath)
    
    
       err_flag = False
       # serialize the checkpoint data
       if not err_flag:
          try:
              package = pickle.dumps(checkpointdata)
          except Exception as e:
              logger.info("%s pid[%i]: error was occured when serializing... [%s]. " %(pname,process_id,str(e)))
              err_flag=True
    
       elapsed_time_mkchkpoint2=time.time()
       # write data
       if not err_flag:
          try:
              if os.path.isfile(writefilenamepath):
                logger.warning("%s pid[%i]: (WRITE WARNING) checkpoint file already exists in [%s] so it will be removed and written." \
                                   %(pname,process_id,writefilenamepath))
                os.remove(writefilenamepath)
              write_checkpoint = open(writefilenamepath, "wb")
              pickle.dump(package, write_checkpoint)
              write_checkpoint.close()
    
          except Exception as e:
              logger.info("%s pid[%i]: checkpoint file write error was occured [%s]. " %(pname,process_id,str(e)))
              err_flag=True
              return 1
    
       elapsed_time_mkchkpoint3=time.time()
       # target_dir backup
       restartdata_dir = os.path.join(checkpoint_dir,"backupdata")
       restartdata_dir = os.path.abspath(restartdata_dir)
       restartdata_dir = os.path.normpath(restartdata_dir) + "_" + checkpoint_route_tag_next_list[route_tag_index]
       restartdata_dir = os.path.normpath(restartdata_dir)
    
       if not os.path.isdir(restartdata_dir): os.mkdir(restartdata_dir)
    
       elapsed_time_mkchkpoint4=time.time()
       # ハードリンクによるrestartdata_dirへの退避
       # 出力ディレクトリにあるcheckpoint以外のディレクトリをバックアップ
#       backuplist = os.listdir(target_dir)
       backuplist = target_list
       for backupdir in backuplist:
           # checkpointは除外
           if backupdir == os.path.basename(checkpoint_dir):
               continue
           else:
               # src_target_dirがバックアップの対象のフルパス
               #src_target_dir = target_dir + "/" + backupdir
               src_target_dir = backupdir
               #if not os.path.isdir(src_target_dir):
               #    continue
    
               args=["cp", "-lr", src_target_dir, restartdata_dir]
               ret = subprocess.call(args)
               logger.info("%s pid[%i]: (WRITE WARNING) target_dir backup %s to %s" \
                               %(pname,process_id,src_target_dir, restartdata_dir))
               logger.warning("%s pid[%i]: (WRITE WARNING) target_dir backup subprocess.call returncode %d" \
                               %(pname,process_id,ret))
    
       if not err_flag:
    
          tag_next_len = len(checkpoint_tag_next_list)
          route_tag_next_len = len(checkpoint_route_tag_next_list)
          if tag_next_len != route_tag_next_len:
              logger.warning("%s pid[%i]: (WRITE WARNING) difference in the length of checkpoint_tag_next_list(%d) and checkpoint_route_tag_next_list(%d)" \
                              %(pname,process_id,tag_next_len,route_tag_next_len))
          elif tag_next_len > 2 or route_tag_next_len > 2:
              logger.warning("%s pid[%i]: (WRITE WARNING) checkpoint_tag_next_list(%d) and checkpoint_route_tag_next_list(%d) greater than 2 " \
                              %(pname,process_id,tag_next_len,route_tag_next_len))
    
          route_tag_next_str = str(checkpoint_route_tag_next_list[route_tag_index])
          tag_next_str = str(checkpoint_tag_next_list[route_tag_index])
          # make restartmasterdata filename
          checkpointmaster_filename = "checkpoint_"+route_tag_next_str+".mst"
          restartinfo_filename = os.path.join(checkpoint_absdir,checkpointmaster_filename)
          restartinfo_filename = os.path.normpath(restartinfo_filename)
    
          # append or write restart info
          restart_info_list  = []
          restart_info_list  = [tag_next_str , writefilenamepath]
        
          restart_info_string = ",".join(restart_info_list)
          restart_info_string += "\n"
    
          try:
              if os.path.isfile(restartinfo_filename):
                 logger.debug("%s pid[%i]: restart master file was already found in [%s]. " %(pname,process_id, restartinfo_filename))
                 write_masterfile = open(restartinfo_filename, "a")
              else:
                 logger.info("%s pid[%i]: restart master file was newly created in [%s]. " %(pname,process_id, restartinfo_filename))
                 write_masterfile = open(restartinfo_filename, "w")
              write_masterfile.write(restart_info_string)
              write_masterfile.close()
          
          except (IOError, OSError) as e:
              logger.error("%s pid[%i]: ERROR! ... write error was occured [%s]. " %(pname,process_id,str(e)))
              err_flag=True
              return 1
       
       elapsed_time_mkchkpoint5=time.time()
       logger.info("%s pid[%i]: write_checkpoint tag %s elapsed_time_mkchkpoint %f[sec]" %(pname,process_id,route_tag_index,elapsed_time_mkchkpoint2-elapsed_time_mkchkpoint1))
       logger.info("%s pid[%i]: write_checkpoint tag %s elapsed_time_write_chkpoint %f[sec]" %(pname,process_id,route_tag_index,elapsed_time_mkchkpoint3-elapsed_time_mkchkpoint2))
       logger.info("%s pid[%i]: write_checkpoint tag %s elapsed_time_mk_targetdir %f[sec]" %(pname,process_id,route_tag_index,elapsed_time_mkchkpoint4-elapsed_time_mkchkpoint3))
       logger.info("%s pid[%i]: write_checkpoint tag %s elapsed_time_hardlink %f[sec]" %(pname,process_id,route_tag_index,elapsed_time_mkchkpoint5-elapsed_time_mkchkpoint4))

   elapsed_time=time.time() - elapsed_time
   logger.info("%s pid[%i]: write_checkpoint elapsed_time %f[sec]" %(pname,process_id,elapsed_time))

   return 0


