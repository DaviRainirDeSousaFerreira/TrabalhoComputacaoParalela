import { io } from "socket.io-client";
import { Worker } from "node:worker_threads";
import os from "node:os";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";

// Endereço público do servidor coordenador.
const URL_SERVIDOR = "https://diff-laws-sept-unless.trycloudflare.com";

// Configurações padrão.
const WORKERS_PADRAO = 2;

// ------------------------------------------------------------
// Argumentos da linha de comando
// ------------------------------------------------------------

const argumentoWorkers = process.argv.find((argumento) =>
  argumento.startsWith("--workers="),
);

const workersSolicitados = argumentoWorkers
  ? Number(argumentoWorkers.split("=")[1])
  : WORKERS_PADRAO;

// Verifica se a quantidade de workers é válida.
if (
  !Number.isInteger(workersSolicitados) ||
  workersSolicitados < 1 ||
  workersSolicitados > 2
) {
  console.error("Informe uma quantidade de workers entre 1 e 2.");
  process.exit(1);
}

const QUANTIDADE_WORKERS = workersSolicitados;

// Ativa as simulações somente quando os argumentos forem informados.
const SIMULAR_FALHA = process.argv.includes("--simulate-failure");
const SIMULAR_DESCONEXAO = process.argv.includes("--simulate-disconnect");

// Controla se cada simulação já foi executada.
let falhaSimuladaExecutada = false;
let desconexaoSimuladaExecutada = false;

// ------------------------------------------------------------
// Identificação do aluno
// ------------------------------------------------------------

const terminal = createInterface({
  input: process.stdin,
  output: process.stdout,
});

function perguntar(pergunta) {
  return new Promise((resolve) => {
    terminal.question(pergunta, (resposta) => {
      resolve(resposta.trim());
    });
  });
}

async function solicitarIdentificacao() {
  const nome = await perguntar("Digite seu nome: ");
  const matricula = await perguntar("Digite sua matrícula: ");
  const codigoTurma = await perguntar("Digite o código da turma: ");

  if (!nome || !matricula || !codigoTurma) {
    throw new Error("Todos os campos de identificação são obrigatórios.");
  }

  return { nome, matricula, codigoTurma };
}

let identificacao;

try {
  identificacao = await solicitarIdentificacao();
} catch (erro) {
  console.error("Não foi possível obter a identificação:", erro.message);
  terminal.close();
  process.exit(1);
}

terminal.close();

const { nome, matricula, codigoTurma } = identificacao;

// ------------------------------------------------------------
// Informações do computador e conexão
// ------------------------------------------------------------

const cpus = os.cpus().length || 1;
const workers = QUANTIDADE_WORKERS;

console.log(
  `Conectando ao servidor ${URL_SERVIDOR} | ${cpus} CPUs lógicas | ${workers} worker(s)`,
);

if (SIMULAR_FALHA) {
  console.log("MODO DE TESTE: FALHA SIMULADA ATIVADA");
}

if (SIMULAR_DESCONEXAO) {
  console.log("MODO DE TESTE: DESCONEXÃO AUTOMÁTICA ATIVADA");
}

const socket = io(URL_SERVIDOR, {
  auth: {
    role: "worker",
    codigoTurma,
  },
  transports: ["websocket"],
  reconnection: true,
  reconnectionDelay: 2000,
  reconnectionDelayMax: 10000,
});

let heartbeat = null;
let current = null;

const threads = new Set();

let encerrando = false;

// ------------------------------------------------------------
// Controle e cancelamento de tarefas
// ------------------------------------------------------------

function cancel(taskEsperada = null) {
  if (taskEsperada && current !== taskEsperada) {
    return false;
  }

  current = null;

  for (const thread of threads) {
    thread.terminate();
  }

  threads.clear();

  return true;
}

function identidadeDaTarefaValida(task) {
  return (
    task &&
    typeof task.taskId === "string" &&
    typeof task.attemptId === "string" &&
    typeof task.experimentId === "string" &&
    task.taskId.length > 0 &&
    task.attemptId.length > 0 &&
    task.experimentId.length > 0
  );
}

// ------------------------------------------------------------
// Execução de Worker Threads
// ------------------------------------------------------------

function runThread(inicio, fim) {
  return new Promise((resolve, reject) => {
    const thread = new Worker(new URL("./worker.js", import.meta.url), {
      workerData: { inicio, fim },
    });

    threads.add(thread);

    let resultadoRecebido = false;
    let resultadoWorker;
    let erroWorker = null;
    let finalizada = false;

    function finalizarComErro(erro) {
      if (finalizada) return;

      finalizada = true;
      reject(erro);
    }

    thread.once("message", (resultado) => {
      resultadoRecebido = true;
      resultadoWorker = resultado;
    });

    thread.once("error", (erro) => {
      erroWorker = erro;
    });

    thread.once("exit", (codigo) => {
      threads.delete(thread);

      if (erroWorker) {
        finalizarComErro(erroWorker);
        return;
      }

      if (codigo !== 0) {
        finalizarComErro(new Error(`Worker encerrado com código ${codigo}.`));
        return;
      }

      if (!resultadoRecebido) {
        finalizarComErro(new Error("Worker terminou sem enviar um resultado."));
        return;
      }

      if (finalizada) return;

      finalizada = true;
      resolve(resultadoWorker);
    });
  });
}

// ------------------------------------------------------------
// Eventos de conexão
// ------------------------------------------------------------

socket.on("connect", () => {
  console.log("CONECTADO — registrando nó...");

  socket.emit("registrar-no", {
    nome,
    matricula,
    plataforma: os.platform(),
    cpus,
    workers,
  });
});

socket.on("registro-confirmado", ({ nodeId, heartbeatMs }) => {
  console.log(`Identificação: Nó #${nodeId}`);
  console.log("Status: AGUARDANDO TAREFA");

  if (heartbeat) {
    clearInterval(heartbeat);
  }

  heartbeat = setInterval(() => {
    if (socket.connected) {
      socket.emit("heartbeat");
    }
  }, heartbeatMs);
});

socket.on("connect_error", (erro) => {
  console.error("Erro ao conectar ao servidor:", erro.message);
});

socket.on("disconnect", (motivo) => {
  console.log("Desconectado do servidor. Motivo:", motivo);

  if (heartbeat) {
    clearInterval(heartbeat);
    heartbeat = null;
  }

  cancel();
});

// ------------------------------------------------------------
// Cancelamento solicitado pelo servidor
// ------------------------------------------------------------

socket.on("tarefa-cancelada", (dados = {}) => {
  if (
    !current ||
    (dados.taskId && dados.taskId !== current.taskId) ||
    (dados.attemptId && dados.attemptId !== current.attemptId)
  ) {
    return;
  }

  const tarefaCancelada = current;

  cancel(tarefaCancelada);

  console.log(
    `Tarefa ${tarefaCancelada.taskId} cancelada pelo servidor. Aguardando nova tarefa.`,
  );
});

// ------------------------------------------------------------
// Recebimento e validação de tarefas
// ------------------------------------------------------------

socket.on("nova-tarefa", async (task) => {
  if (!identidadeDaTarefaValida(task)) {
    console.error("Tarefa inválida recebida: identificadores ausentes.");
    return;
  }

  if (current) {
    console.error("Tarefa recusada: cliente já está ocupado.");

    socket.emit("falha-tarefa", {
      taskId: task.taskId,
      attemptId: task.attemptId,
    });

    return;
  }

  if (
    task.operacao !== "sqrt_sum" ||
    !Number.isSafeInteger(task.inicio) ||
    !Number.isSafeInteger(task.fim) ||
    task.inicio < 1 ||
    task.fim < task.inicio ||
    task.fim > 1_000_000_000
  ) {
    console.error("Tarefa recusada: operação ou intervalo inválido.");

    socket.emit("falha-tarefa", {
      taskId: task.taskId,
      attemptId: task.attemptId,
    });

    return;
  }

  // ----------------------------------------------------------
  // Simulação controlada de falha
  // ----------------------------------------------------------

  if (SIMULAR_FALHA && !falhaSimuladaExecutada) {
    falhaSimuladaExecutada = true;

    console.log("\n========== FALHA SIMULADA ==========");
    console.log("Tarefa:", task.taskId);
    console.log("Tentativa:", task.attemptId);
    console.log("Intervalo:", task.inicio, "até", task.fim);
    console.log("A tarefa NÃO será processada.");
    console.log("Enviando notificação de falha ao servidor...");

    socket.emit("falha-tarefa", {
      taskId: task.taskId,
      attemptId: task.attemptId,
    });

    console.log("FALHA SIMULADA ENVIADA");
    console.log("Aguardando outra tarefa.");
    console.log("====================================");

    return;
  }

  // ----------------------------------------------------------
  // Processamento normal
  // ----------------------------------------------------------

  current = task;

  console.log("\n========== NOVA TAREFA RECEBIDA ==========");
  console.log("Task:", task.taskId);
  console.log("Tentativa:", task.attemptId);
  console.log("Intervalo:", task.inicio, "até", task.fim);
  console.log("Processando...");

  const start = performance.now();

  try {
    const count = task.fim - task.inicio + 1;
    const parallelism = Math.min(workers, count);

    const base = Math.floor(count / parallelism);
    const remainder = count % parallelism;

    let inicio = task.inicio;

    const promises = [];

    for (let index = 0; index < parallelism; index++) {
      const tamanho = base + (index < remainder ? 1 : 0);
      const fim = inicio + tamanho - 1;

      promises.push(runThread(inicio, fim));

      inicio = fim + 1;
    }

    // ----------------------------------------------------------
    // Simulação de desconexão durante o processamento
    // ----------------------------------------------------------

    if (SIMULAR_DESCONEXAO && !desconexaoSimuladaExecutada) {
      desconexaoSimuladaExecutada = true;

      console.log("\n========== DESCONEXÃO SIMULADA ==========");
      console.log("Os workers começaram a processar a tarefa.");
      console.log("A conexão será encerrada em 100 ms.");

      setTimeout(() => {
        if (current !== task || !socket.connected) {
          return;
        }

        console.log("\nDESCONECTANDO AUTOMATICAMENTE...");
        console.log("O servidor deverá recuperar a tarefa.");

        socket.disconnect();

        // Aguarda o encerramento da conexão antes de finalizar.
        setTimeout(() => {
          process.exit(0);
        }, 500);
      }, 100);
    }

    const partials = await Promise.all(promises);

    if (current !== task || !socket.connected) {
      return;
    }

    const processingMs = performance.now() - start;

    const resultado = partials.reduce(
      (soma, parcial) => soma + parcial.resultado,
      0,
    );

    const kernelMs = Math.max(...partials.map((parcial) => parcial.kernelMs));

    if (!Number.isFinite(resultado)) {
      throw new Error("O cálculo produziu um resultado numérico inválido.");
    }

    console.log("\nTAREFA FINALIZADA");
    console.log("Tempo local:", processingMs.toFixed(2), "ms");
    console.log("Resultado:", resultado);
    console.log("Enviando resultado...");

    socket.timeout(10000).emit(
      "resultado-tarefa",
      {
        taskId: task.taskId,
        attemptId: task.attemptId,
        experimentId: task.experimentId,
        resultado,
        processingMs,
        kernelMs,
        workersUsed: parallelism,
      },
      (erro, ack) => {
        if (current !== task) {
          return;
        }

        current = null;

        if (!erro && ack?.ok) {
          console.log("RESULTADO CONFIRMADO PELO SERVIDOR");
          console.log("Aguardando próxima tarefa.");
        } else {
          console.log(
            "Resultado não confirmado:",
            ack?.error ?? erro?.message ?? "Resposta inválida do servidor",
          );
        }
      },
    );
  } catch (erro) {
    if (current !== task) {
      return;
    }

    console.error("Falha no processamento:", erro.message);

    cancel(task);

    if (socket.connected) {
      socket.emit("falha-tarefa", {
        taskId: task.taskId,
        attemptId: task.attemptId,
      });
    }
  }
});

// ------------------------------------------------------------
// Encerramento seguro do cliente
// ------------------------------------------------------------

function encerrarCliente(signal) {
  if (encerrando) return;

  encerrando = true;

  console.log(`\nEncerrando cliente (${signal})...`);

  if (heartbeat) {
    clearInterval(heartbeat);
    heartbeat = null;
  }

  cancel();

  socket.disconnect();

  process.exit(0);
}

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => encerrarCliente(signal));
}
