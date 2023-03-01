# KVM Backup
``` bash
#!/bin/bash
tmp_path=/root/back/tmp

rm -f $tmp_path/*
virsh dumpxml mgm > $tmp_path/mgm-`date +\%Y_%m_%d`.xml
s3cmd put $tmp_path/mgm-`date +\%Y_%m_%d`.xml s3://kvm/mgm/
s3cmd put /srv/kvm/sas/mgm.qcow2 s3://kvm/mgm/mgm-`date +\%Y_%m_%d`.qcow2
```

# PSQL Backup
``` bash
#!/bin/bash
path=/srv/disk/pgsql
rm -f $path/tmp/*
pg_dump -U eigm_ritm2 eigm_ritm2 | gzip -c > $path/tmp/eigm_ritm2-`date +\%Y_%m_%d`.sql.gz
s3cmd put --recursive $path/tmp/* s3://postgres-dump/ritm2/
```

# MariaDB Backup
``` bash
#!/bin/bash
path=/root/backup
rm -f $path/tmp/*
$path/check_econo
$path/check_edulogic
$path/check_spp42
mysqldump -u root econo_wp | gzip -c > $path/tmp/econo_wp-`date +\%Y_%m_%d`.sql.gz
mysqldump -u root edulogicweb | gzip -c  > $path/tmp/edulogicweb-`date +\%Y_%m_%d`.sql.gz
mysqldump -u root spp42_w42press | gzip -c > $path/tmp/spp42_w42press-`date +\%Y_%m_%d`.sql.gz
tar -cvzf $path/tmp/econometrics.com-`date +\%Y_%m_%d`.tar.gz /srv/disk/www/econo/
tar -cvzf $path/tmp/edulogic.com-`date +\%Y_%m_%d`.tar.gz /srv/disk/www/edulogic/
tar -cvzf $path/tmp/spp42.com.tr-`date +\%Y_%m_%d`.tar.gz /srv/disk/www/spp42/
s3cmd put --recursive $path/tmp/* s3://wordpress/
```

# Git S3 Clean
``` bash
#!/bin/bash
s3cmd ls --recursive s3://gitlab/* | grep tar | while read -r line;
do
        createDate=`echo $line|awk {'print $1" "$2'}`
        createDate=`date -d"$createDate" +%s`
        olderThan=`date --date "7 days ago" +%s`
        if [[ $createDate -lt $olderThan ]]
                then
                        fileName=`echo $line|awk {'print $4'}`
                        if [[ $fileName != "" ]]
                                then
                                        s3cmd del $fileName
                        fi
        fi
done;
```

# PSQL S3 Clean
``` bash
#!/bin/bash
s3cmd ls --recursive s3://postgres-dump/ritm2/* | grep gz | while read -r line;
do
        createDate=`echo $line|awk {'print $1" "$2'}`
        createDate=`date -d"$createDate" +%s`
        olderThan=`date --date "2 month ago" +%s`
        if [[ $createDate -lt $olderThan ]]
                then
                        fileName=`echo $line|awk {'print $4'}`
                        if [[ $fileName != "" ]]
                                then
                                        s3cmd del $fileName
                        fi
        fi
done;
```
