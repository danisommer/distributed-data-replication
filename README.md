# Replicação de dados (modelo Pull, eleição estilo Raft, gRPC)

4 nós idênticos (portas 50051–50054) + cliente CLI. Quórum fixo de 3 nós.

## Preparação

```
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

(Os stubs `raft_pb2*.py` já vêm no zip; o último comando só é necessário se o `.proto` mudar.)

## Subir o cluster

Um terminal por nó, a partir da raiz do projeto (`N` = 1, 2, 3, 4):

```
python node.py --id N
```

Cada nó imprime papel, termo e índice de commit a cada mudança e a cada 2 s. O estado fica em `data/nodeN/` (`state.json`, `log.jsonl`, `db.json`). Para recomeçar do zero, pare os nós e apague `data/`.

Para os cenários 4B e 5 suba os nós com replicação lenta (abre uma janela de ~2 s entre gravar e replicar):

```
python node.py --id N --fetch-ms 2000 --hb-timeout-ms 6000 --election-min-ms 6000 --election-max-ms 12000 --commit-wait-s 15
```

Aguarde a eleição (alguns segundos) e confirme com `python client.py status`.

## Cliente

```
python client.py write CHAVE VALOR     # segue o redirecionamento até o líder
python client.py read CHAVE [--node N] # sem --node, consulta os 4 nós
python client.py status [--node N]     # papel, termo, commit, log (comitado ou não) e banco
```

Derrubar um nó = Ctrl+C no terminal dele; religar = rodar o mesmo comando de novo.

## Cenários

**1. Escrita normal e leitura distribuída** (parâmetros padrão)
```
python client.py write chave valor
python client.py read chave
```
Esperado: `OK ... comitado`, e a chave aparece nos 4 nós.

**2. Eleição por queda do líder** (padrão)
1. `python client.py status` e identifique o líder.
2. Ctrl+C no líder.
3. `python client.py status` após alguns segundos.

Esperado: novo líder com termo incrementado; o log comitado é preservado.

**3. Perda de quórum** (padrão)
1. Com o líder e mais um nó vivo (2 no total), aguarde ~1 s.
2. `python client.py write k2 v2`

Esperado: `REJEITADO pelo lider ...: sem quorum: 2 no(s) ativo(s), minimo 3`. Ao religar um nó, a mesma escrita é comitada.

**4A. Réplica desatualizada** (padrão)
1. Derrube um seguidor.
2. `python client.py write k3 v3`
3. Religue o seguidor e rode `python client.py status --node N`.

Esperado: o nó volta como seguidor e recebe as entradas faltantes por `Fetch`, ficando com log e banco iguais aos dos demais.

**4B. Truncamento por divergência** (parâmetros lentos; líder = N1 no exemplo)
1. Grave e derrube o líder logo após ele gravar no log local, antes da replicação:
   ```
   python client.py write k1 v1 &
   until grep -q '"key": "k1"' data/node1/log.jsonl; do sleep 0.02; done; pkill -f "node.py --id 1"
   ```
2. `python client.py status` e confirme que `k1` não está no log de nenhum outro nó. Se estiver, apague `data/` e refaça.
3. Aguarde o novo líder (6–12 s) e rode `python client.py write k2 v2` até comitar.
4. Religue o nó derrubado (mesmos parâmetros lentos) e rode `python client.py status --node 1`.

Esperado: no terminal do nó religado, `log truncado em ...`; o log final não tem `k1` e é igual ao do novo líder.

**5. Isolamento de dados não comitados** (parâmetros lentos)
```
python client.py write k5 v5 &
for i in 1 2 3 4; do sleep 1; echo "--- t=${i}s"; python client.py read k5 --node 1; python client.py status --node 1 | grep -E "log\[|banco"; done
```
Use uma chave inédita. Esperado: o log do nó mostra `k5=v5 [NAO comitada]` enquanto o `read` devolve `nao encontrado` e o banco não tem `k5`; no `Fetch` seguinte o commit chega e a entrada passa a `[comitada]`.

## Observações

- Cada eleição grava uma entrada `(noop)` no log; ela não vai para o banco.
- Sem pré-voto: um nó que fica isolado/atrasado incrementa o termo sem parar e pode forçar uma troca de líder ao voltar. Subir os 4 nós em sequência, sem demora, evita isso.
- Reenviar uma escrita após falha pode gravar a mesma chave mais de uma vez no log (o estado final do banco é o mesmo).
