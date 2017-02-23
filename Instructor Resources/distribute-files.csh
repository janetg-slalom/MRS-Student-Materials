#!/usr/bin/env tcsh

set packagedirname = $1

set homedir = "/home"

if ( $#argv > 1 ) then
    ## used for single user:
    set users = $2
else
    ## general do it for all users
    pushd $homedir
	set users = `ls -1`
    popd
endif

foreach user ($users)
    echo Distributing to $user
    sudo rsync --exclude output -auvz $packagedirname/ ${homedir}/$user/$packagedirname
    echo Setting permissions for $user
    sudo chown -R $user\:$user ${homedir}/$user/$packagedirname
end


echo Done!
