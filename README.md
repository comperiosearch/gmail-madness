### Intro

This repo holds a script `mbox2es.py` that takes a .mbox-formatted file containing a collection of elelctronic mail, and indexes it to a elasticsearch instance running at localhost:9200 to the index `mail`. It also holds a script ```gmvault2es.py``` that takes the directory structure created by the tool [gmvault](http://gmvault.org/) and indexes it to the same elasticsearch index.

Some of the following stuff might seem technical, but it's really not that hard. Stay with us! We can help.

### Prerequisites

Make sure your computer has ```git``` installed and that you are able to clone repositories.

1. A .mbox-formatted file with all your emails. You can get this from GMail [here](https://www.google.com/settings/takeout/custom/gmail). Alternatively, you can download your email with gmvault, but it is very slow. Other email providers may also give you such a file.
2. A virtualization tool, we recommend [Virtualbox](https://www.virtualbox.org/)
3. [Vagrant](https://www.vagrantup.com/downloads.html) must be installed as well.
4. [This repo needs to be cloned to your machine](https://github.com/comperiosearch/vagrant-elk-box/tree/gmail). You do that by running `git clone https://github.com/comperiosearch/vagrant-elk-box.git` somewhere on your computer.
5. Make sure you are on branch ```gmail``` (run `git checkout gmail`)
6. Run ```vagrant up```from the root of vagrant-elk-box folder.


### Getting started

If `vagrant up` succeded and you have been able to download your email archive we are good to go! Move your .mbox-archive to the vagrant-elk-box folder on your computer (`mv ~/path/to/my-email.mbox ~/path/to/vagrant-elk-box/`). Cool. `cd` to the same directory: `cd ~/path/to/vagrant-elk-box`. We may now type `vagrant ssh` and we are ssh-ing into our virtual machine running Ubuntu 14.04. This is when the fun part starts. Now you may type `cd /vagrant/gmail-madness` to go to this repo cloned into the VM. Now the magic starts if you run `python mbox2es.py ../my-email.mbox` you data will start to get indexed. You may look at your data by going to a browser and typing `localhost:5601`.

### Debugging

* If Gmvault fails to connect to Gmail, it might be due to your anti virus.
* If you have a Windows machine some of this can be hard, but Sébastien can help you!
