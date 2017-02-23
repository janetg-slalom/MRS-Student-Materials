for i in {1..3}; do 
  u=`openssl rand -hex 2`; 
  sudo useradd user$u; 
  p=`openssl rand -hex 5`; 
  echo $p | sudo passwd user$u --stdin; 
  echo user$u, $p >> 'usersinfo.csv';
done
