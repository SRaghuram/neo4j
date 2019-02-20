
## TeamCity
These are only used in TC build, please do not disturb them.
* vildvittra\d+
  ```
  CPU: 1 x 4-core Xeon Skylake-DT
  RAM: 64GB
  STORAGE: 480GB SSD
  STORAGE: 3TB spinny-disk
  ```
* skalleper\d+
  ```
  CPU: 1(?) x Xeon Gold 6152 (22C/44T) @ 2.1Ghz
  RAM:  512GB
  STORAGE: 3x960GB M.2 SSD
  STORAGE: 1x6TB spinny-disk
  ```
* borka
  ```
  RAM: 512GB
  STORAGE: 37TB SSD
  ```
## Miscellaneous
These are free to use for prototyping and similar work.
Check slack #malmo-server-mgmt to coordinate with other developers.

* mattis
  ```
  CPU: Xeon E7-8870 v3 @ 2.1Ghz CPU (72C/144T)
  RAM: 1TB DDR4
  STORAGE: 1.8TB PCIE-SSD
  STORAGE: 5.5TB spinny-disk
  ```
* knotas (formally skalleper)
  ```
  CPU: 4 x Xeon E5-4610 v2 @ 2.30GHz CPU (32C/64T)
  RAM: 367GB

  SPINNIES
  sdb1    100gb    raid1 /
  sbc1    100gb    raid1 /
  sdb2    ~2.6T    raid0 /home
  sdc2    ~7.2T    raid0 /home

  SSDIES
  ssd(sda)  558G   plain /ssd
  nvme0n1   372G   plain /pcie-ssd
  ```
## Accessing the servers
In order to be able to login to the servers, your public key have to be added to the servers. This is managed by external consultants that can be found in the #malmo-server-mgmt channel. Send one of them a message, e.g:

```
@thomas.danielsson Could you please add me to `%serverName%`?


ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCpnRlOSlJszZsZBkeSvz0DcmFOIj+e4RvTxB1UTREgo0QCEMHNMa9YT4CQhUl4NZgzLVMeChwUF4LrNBCAiX/rZjKJf2ihzYV7+grjEq2DLa/k60l3f3HHsKh8Ox2VOX4aptBaoVG7eNyIL1fopZbZdDXiSiC15SCgp1UJzVxtTxBnGW7rg+/iF2uIxo6O7Kw2wWz97lqQ59rqC08TEvvuwa0H4HbOZ19A/T6iRRNzgW8F0Ukz9EcCWWr1Zq6oLsODWGqPUawDJUnA81vtBhiToKk/RxlfhVDyr+Pm7eUDnDNsm/W+dnt1EBp/UsPAsnx25H8F+Z6Zqxhp1HiXhucqMEzXNqP4/OYK03FEwchOCPvIlRtk906wSlxnW0VhNmB0DQwsKgk+fMnTumFPXO8U3+aMkZDEt82GxvQ4cNKwHlRAUyEOFdGGKBca3dob0sB/fwiXcB6PqEeGMsdypWJQbAt0DOyr8kb78Es+qNGiSQF+E0fY+M658tkX6qfB5U2pQ3fSazohiOJSYAv2hy0tTFfs8d8g1ajPuiBYLEXeXyKVl3JZ5K5muRLfQVHAI3mWXfkWcU2tVnk00RCtsheeZvs3MOqXof3WI8cPQlEIGv3KO6Esa2fdhgULvc0EbVW7hjBmonTaR75qXwWHlnz9RBoAEmE/gk4lp1yviT8TkQ== anton.klaren@neotechnology.com

```


After your key is added, thank the lovely people running our server, before you go on to login to the server.
### Before ssh
Before accessing the server, make sure that it's not already occupied. This is indicated by the channel topic where the name in the brackets indicated the current owner. Empty brackets mean it's up for grabs.

To connect to the server with ssh, you can run in your favorite terminal: 
ssh neo@%serverName%.malmo.neohq.net

After you're done with your task, don't forget to clean up after to leave the server in a nice and tidy state :)
And remove your name from the brackets in the channel topic.
