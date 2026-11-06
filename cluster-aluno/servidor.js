import { createServer } from "node:http";
import { Server } from "socket.io";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

const PORT = 3000;
const HEARTBEAT_MS = 10000;
const TIMEOUT_TAREFA_MS = 30000;
const INTERVALO_VERIFICACAO_MS = 5000;

const nosConectados = new Map();

let proximoNodeId = 1;
let experimentoAtual = null;

// ------------------------------------------------------------
// Servidor HTTP e rotas
// ------------------------------------------------------------

const httpServer = createServer((req, res) => {
  console.log("Requisição recebida:", req.method, req.url);

  if (req.method === "GET" && req.url.startsWith("/iniciar")) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    iniciarExperimento(url, res);
    return;
  }

  if (req.method === "GET" && req.url.startsWith("/centralizado")) {
    executarCentralizado(req, res);
    return;
  }

  if (req.method === "GET" && req.url.split("?")[0] === "/status") {
    const tarefas = experimentoAtual
      ? [...experimentoAtual.tarefas.values()]
      : [];

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        nosConectados: nosConectados.size,
        experimentoEmAndamento: experimentoAtual !== null,
        tarefas: experimentoAtual
          ? {
              total: tarefas.length,
              executando: tarefas.filter(
                (tarefa) => tarefa.status === "executando",
              ).length,
              pendentes: tarefas.filter(
                (tarefa) => tarefa.status === "pendente",
              ).length,
              concluidas: tarefas.filter(
                (tarefa) => tarefa.status === "concluida",
              ).length,
            }
          : null,
      }),
    );
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Rota não encontrada");
});

// ------------------------------------------------------------
// Socket.IO
// ------------------------------------------------------------

const io = new Server(httpServer, {
  cors: {
    origin: "*",
  },
});

io.on("connection", (socket) => {
  console.log(`Nova conexão recebida: ${socket.id}`);

  socket.on("registrar-no", (dados = {}) => {
    const nodeId = proximoNodeId++;

    nosConectados.set(socket.id, {
      nodeId,
      socketId: socket.id,
      nome: dados.nome ?? "Não informado",
      matricula: dados.matricula ?? "Não informada",
      plataforma: dados.plataforma ?? "Não informada",
      cpus: dados.cpus ?? 0,
      workers: dados.workers ?? 0,
      ultimoHeartbeat: Date.now(),
      tarefaAtual: null,
    });

    console.log(`\nNó #${nodeId} registrado.`);
    console.log(`Nome: ${dados.nome ?? "Não informado"}`);
    console.log(`CPUs: ${dados.cpus ?? 0}`);
    console.log(`Workers: ${dados.workers ?? 0}`);
    console.log(`Total de nós: ${nosConectados.size}`);

    socket.emit("registro-confirmado", {
      nodeId,
      heartbeatMs: HEARTBEAT_MS,
    });

    if (experimentoAtual) {
      distribuirTarefasPendentes(experimentoAtual);
    }
  });

  socket.on("heartbeat", () => {
    const no = nosConectados.get(socket.id);

    if (no) {
      no.ultimoHeartbeat = Date.now();
    }
  });

  // ----------------------------------------------------------
  // Recebimento de resultados
  // ----------------------------------------------------------

  socket.on("resultado-tarefa", (dados = {}, confirmar) => {
    const responder = (resposta) => {
      if (typeof confirmar === "function") {
        confirmar(resposta);
      }
    };

    const experimento = experimentoAtual;

    if (!experimento) {
      responder({ ok: false, error: "Nenhum experimento ativo" });
      return;
    }

    const tarefa = experimento.tarefas.get(dados.taskId);

    if (
      !tarefa ||
      tarefa.status !== "executando" ||
      tarefa.concluida ||
      tarefa.attemptId !== dados.attemptId ||
      tarefa.experimentId !== dados.experimentId ||
      tarefa.socketId !== socket.id
    ) {
      responder({
        ok: false,
        error: "Resultado inválido ou tentativa antiga",
      });
      return;
    }

    if (!Number.isFinite(dados.resultado)) {
      responder({ ok: false, error: "Resultado numérico inválido" });
      return;
    }

    tarefa.concluida = true;
    tarefa.status = "concluida";
    tarefa.resultado = dados.resultado;
    tarefa.processingMs = dados.processingMs;
    tarefa.kernelMs = dados.kernelMs;
    tarefa.workersUsed = dados.workersUsed;

    const no = nosConectados.get(socket.id);

    if (no && no.tarefaAtual === tarefa.taskId) {
      no.tarefaAtual = null;
    }

    console.log(`\nResultado recebido do Nó #${tarefa.nodeId}`);
    console.log(`Intervalo: ${tarefa.inicio} até ${tarefa.fim}`);
    console.log(`Resultado parcial: ${dados.resultado}`);

    responder({ ok: true });

    distribuirTarefasPendentes(experimento);
    finalizarExperimentoSeCompleto(experimento);
  });

  // ----------------------------------------------------------
  // Recebimento de falhas
  // ----------------------------------------------------------

  socket.on("falha-tarefa", (dados = {}) => {
    const experimento = experimentoAtual;

    if (!experimento) {
      return;
    }

    const tarefa = experimento.tarefas.get(dados.taskId);

    if (
      !tarefa ||
      tarefa.status !== "executando" ||
      tarefa.attemptId !== dados.attemptId ||
      tarefa.socketId !== socket.id
    ) {
      console.log("Notificação de falha ignorada: tentativa inválida.");
      return;
    }

    const no = nosConectados.get(socket.id);

    console.log(`\nFalha informada pelo Nó #${no?.nodeId ?? "desconhecido"}.`);

    recolocarTarefaComoPendente(
      experimento,
      tarefa,
      "O computador informou uma falha durante o processamento.",
    );

    distribuirTarefasPendentes(experimento);
  });

  // ----------------------------------------------------------
  // Desconexão de computadores
  // ----------------------------------------------------------

  socket.on("disconnect", (motivo) => {
    const no = nosConectados.get(socket.id);

    if (no) {
      console.log(`\nNó #${no.nodeId} desconectado. Motivo: ${motivo}`);

      if (experimentoAtual && no.tarefaAtual) {
        const tarefa = experimentoAtual.tarefas.get(no.tarefaAtual);

        if (tarefa) {
          recolocarTarefaComoPendente(
            experimentoAtual,
            tarefa,
            `O Nó #${no.nodeId} perdeu a conexão.`,
          );
        }
      }

      nosConectados.delete(socket.id);
    }

    console.log(`Total de nós conectados: ${nosConectados.size}`);

    if (experimentoAtual) {
      distribuirTarefasPendentes(experimentoAtual);
    }
  });
});

// ------------------------------------------------------------
// Distribuição de tarefas
// ------------------------------------------------------------

function distribuirTarefasPendentes(experimento) {
  if (!experimento || experimento !== experimentoAtual) {
    return;
  }

  const tarefasPendentes = [...experimento.tarefas.values()].filter(
    (tarefa) => tarefa.status === "pendente",
  );

  for (const tarefa of tarefasPendentes) {
    // Procura um computador livre que ainda não tenha falhado
    // ao executar esta tarefa.
    const noDisponivel = [...nosConectados.values()].find(
      (no) => no.tarefaAtual === null && !tarefa.nosQueFalharam.has(no.nodeId),
    );

    if (!noDisponivel) {
      console.log(
        `Tarefa ${tarefa.taskId} permanece pendente: nenhum nó elegível disponível.`,
      );
      continue;
    }

    tarefa.attemptId = randomUUID();
    tarefa.socketId = noDisponivel.socketId;
    tarefa.nodeId = noDisponivel.nodeId;
    tarefa.tentativas++;
    tarefa.iniciadaEm = Date.now();
    tarefa.status = "executando";
    tarefa.concluida = false;

    // Registra que este computador já recebeu a tarefa.
    tarefa.nosQueFalharam.add(noDisponivel.nodeId);

    noDisponivel.tarefaAtual = tarefa.taskId;

    console.log(`\nRedistribuindo tarefa ${tarefa.taskId}`);
    console.log(`Destino: Nó #${tarefa.nodeId}`);
    console.log(`Tentativa: ${tarefa.tentativas}`);
    console.log(`Intervalo: ${tarefa.inicio} até ${tarefa.fim}`);

    io.to(noDisponivel.socketId).emit("nova-tarefa", {
      operacao: "sqrt_sum",
      taskId: tarefa.taskId,
      attemptId: tarefa.attemptId,
      experimentId: tarefa.experimentId,
      inicio: tarefa.inicio,
      fim: tarefa.fim,
    });
  }
}

// ------------------------------------------------------------
// Recuperação de tarefas
// ------------------------------------------------------------

function recolocarTarefaComoPendente(experimento, tarefa, motivo) {
  if (
    !experimento ||
    experimento !== experimentoAtual ||
    !tarefa ||
    tarefa.status !== "executando"
  ) {
    return;
  }

  const socketIdAnterior = tarefa.socketId;
  const nodeIdAnterior = tarefa.nodeId;
  const noAnterior = nosConectados.get(socketIdAnterior);

  // O computador que falhou não poderá receber novamente esta tarefa.
  if (nodeIdAnterior !== null) {
    tarefa.nosQueFalharam.add(nodeIdAnterior);
  }

  if (noAnterior) {
    io.to(socketIdAnterior).emit("tarefa-cancelada", {
      taskId: tarefa.taskId,
      attemptId: tarefa.attemptId,
    });

    if (noAnterior.tarefaAtual === tarefa.taskId) {
      noAnterior.tarefaAtual = null;
    }
  }

  console.log(`\nRECUPERANDO TAREFA: ${tarefa.taskId}`);
  console.log(`Nó anterior: #${nodeIdAnterior}`);
  console.log(`Motivo: ${motivo}`);
  console.log(`Intervalo: ${tarefa.inicio} até ${tarefa.fim}`);

  tarefa.socketId = null;
  tarefa.nodeId = null;
  tarefa.iniciadaEm = null;
  tarefa.status = "pendente";
}

// ------------------------------------------------------------
// Verificação de timeout
// ------------------------------------------------------------

setInterval(() => {
  const experimento = experimentoAtual;

  if (!experimento) {
    return;
  }

  const agora = Date.now();
  let encontrouTarefaExpirada = false;

  for (const tarefa of experimento.tarefas.values()) {
    if (
      tarefa.status === "executando" &&
      tarefa.iniciadaEm !== null &&
      agora - tarefa.iniciadaEm >= TIMEOUT_TAREFA_MS
    ) {
      recolocarTarefaComoPendente(
        experimento,
        tarefa,
        `Tempo limite de ${TIMEOUT_TAREFA_MS} ms excedido.`,
      );

      encontrouTarefaExpirada = true;
    }
  }

  if (encontrouTarefaExpirada) {
    distribuirTarefasPendentes(experimento);
  }
}, INTERVALO_VERIFICACAO_MS);

// ------------------------------------------------------------
// Criação de experimentos
// ------------------------------------------------------------

function iniciarExperimento(url, res) {
  if (experimentoAtual) {
    res.writeHead(409, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        erro: "Já existe um experimento em andamento",
      }),
    );
    return;
  }

  const nos = [...nosConectados.values()].filter(
    (no) => no.tarefaAtual === null,
  );

  if (nos.length === 0) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ erro: "Nenhum nó disponível" }));
    return;
  }

  const inicio = Number(url.searchParams.get("inicio") ?? 1);
  const fim = Number(url.searchParams.get("fim") ?? 1000000);

  if (
    !Number.isSafeInteger(inicio) ||
    !Number.isSafeInteger(fim) ||
    inicio < 1 ||
    fim < inicio ||
    fim > 1000000000
  ) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ erro: "Intervalo inválido" }));
    return;
  }

  const experimentId = randomUUID();
  const quantidade = fim - inicio + 1;
  const quantidadeNos = Math.min(nos.length, quantidade);
  const base = Math.floor(quantidade / quantidadeNos);
  const resto = quantidade % quantidadeNos;

  const experimento = {
    experimentId,
    inicio,
    fim,
    tarefas: new Map(),
    totalNos: quantidadeNos,
    iniciadoEm: Date.now(),
  };

  experimentoAtual = experimento;

  let proximoInicio = inicio;

  console.log("\n========== EXPERIMENTO INICIADO ==========");
  console.log(`Experimento: ${experimentId}`);
  console.log(`Intervalo total: ${inicio} até ${fim}`);
  console.log(`Nós participantes: ${quantidadeNos}`);

  for (let i = 0; i < quantidadeNos; i++) {
    const tamanho = base + (i < resto ? 1 : 0);
    const proximoFim = proximoInicio + tamanho - 1;
    const taskId = randomUUID();
    const attemptId = randomUUID();

    const tarefa = {
      taskId,
      attemptId,
      experimentId,
      socketId: nos[i].socketId,
      nodeId: nos[i].nodeId,
      inicio: proximoInicio,
      fim: proximoFim,
      concluida: false,
      status: "executando",
      tentativas: 1,
      iniciadaEm: Date.now(),

      // Guarda os nós que já receberam esta tarefa.
      nosQueFalharam: new Set([nos[i].nodeId]),
    };

    experimento.tarefas.set(taskId, tarefa);
    nos[i].tarefaAtual = taskId;

    console.log(
      `Enviando tarefa ao Nó #${tarefa.nodeId}: ${proximoInicio} até ${proximoFim}`,
    );

    io.to(tarefa.socketId).emit("nova-tarefa", {
      operacao: "sqrt_sum",
      taskId,
      attemptId,
      experimentId,
      inicio: proximoInicio,
      fim: proximoFim,
    });

    proximoInicio = proximoFim + 1;
  }

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({
      mensagem: "Experimento iniciado",
      experimentId,
      intervalo: { inicio, fim },
      nosParticipantes: quantidadeNos,
    }),
  );
}

// ------------------------------------------------------------
// Finalização de experimentos
// ------------------------------------------------------------

function finalizarExperimentoSeCompleto(experimento) {
  if (!experimento || experimento !== experimentoAtual) {
    return;
  }

  const tarefas = [...experimento.tarefas.values()];

  if (
    tarefas.length === 0 ||
    !tarefas.every((tarefa) => tarefa.status === "concluida")
  ) {
    return;
  }

  const resultadoTotal = tarefas.reduce(
    (soma, tarefa) => soma + tarefa.resultado,
    0,
  );

  const tempoTotalMs = Date.now() - experimento.iniciadoEm;

  console.log("\n========== EXPERIMENTO FINALIZADO ==========");
  console.log(`Resultado total: ${resultadoTotal}`);
  console.log(`Tempo total: ${tempoTotalMs} ms`);
  console.log(`Nós participantes inicialmente: ${experimento.totalNos}`);
  console.log("============================================");

  experimentoAtual = null;
}

// ------------------------------------------------------------
// Execução centralizada para comparação
// ------------------------------------------------------------

function executarCentralizado(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const inicio = Number(url.searchParams.get("inicio") ?? 1);
  const fim = Number(url.searchParams.get("fim") ?? 1000000);

  if (
    !Number.isSafeInteger(inicio) ||
    !Number.isSafeInteger(fim) ||
    inicio < 1 ||
    fim < inicio ||
    fim > 1000000000
  ) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ erro: "Intervalo inválido" }));
    return;
  }

  console.log("\n========== CÁLCULO CENTRALIZADO ==========");
  console.log(`Intervalo: ${inicio} até ${fim}`);

  const inicioTempo = performance.now();
  let resultado = 0;

  for (let i = inicio; i <= fim; i++) {
    resultado += Math.sqrt(i);
  }

  const tempoMs = performance.now() - inicioTempo;

  console.log(`Resultado: ${resultado}`);
  console.log(`Tempo centralizado: ${tempoMs.toFixed(2)} ms`);
  console.log("==========================================");

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({
      tipo: "centralizado",
      inicio,
      fim,
      resultado,
      tempoMs,
    }),
  );
}

// ------------------------------------------------------------
// Inicialização
// ------------------------------------------------------------

httpServer.listen(PORT, () => {
  console.log(`Servidor coordenador executando na porta ${PORT}`);
});
