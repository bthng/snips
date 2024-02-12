File ~/.ssh/config:
```
# Default github account: ytoast
Host github.com
  AddKeysToAgent yes
  IdentityFile ~/.ssh/...
  HostName github.com
  IdentitiesOnly yes

# Other github account: test
Host github.com-test
  HostName github.com
  IdentityFile ~/.ssh/id_rsa_test
  IdentitiesOnly yes
```

```shell
ssh-add /Users/bthng/.ssh/id_rsa_test
```

```shell
git remote set-url origin git@github.com-test:user/repo.git
```
