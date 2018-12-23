# 使い方

## Prometheus&Grafana in Dockerを起動
```
some/path/visualize_scripts/frugalos_on_mac$ docker-compose -f docker-compose.yml up
```
を実行する。この段階で次のようになっている:
- 'localhost:3001' で Grafana（ユーザとパスワードはなんでも良いはず　ダメなら `admin/admin`)

## Frugalosを動かす
次にfrugalosを実行してみる。例えば付属の`example.sh`を使うと便利である。
ただし、`example.sh`はtmuxの内部で叩くこと
```
some/path/visualize_scripts/frugalos_on_mac$ ./example.sh <- tmux中で
```

## Grafanaにダッシュボード登録
このPR https://github.com/frugalos/frugalos/pull/25 のダッシュボードを利用する。
既に `frugalos-dashboard.json` があるので、 `cat frugalos-dashboard.json | pbcopy` などして、
Grafanaのダッシュボード登録ページに貼り付ければ良い。
