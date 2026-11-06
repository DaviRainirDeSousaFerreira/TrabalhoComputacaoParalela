// Importa parentPort para enviar mensagens ao processo principal.
// Importa workerData para receber os dados enviados pelo processo principal.
import { parentPort, workerData } from "node:worker_threads";

// Importa performance para medir o tempo de execução do cálculo.
import { performance } from "node:perf_hooks";

// ------------------------------------------------------------
// Validação dos dados recebidos
// ------------------------------------------------------------

// Verifica se o Worker foi iniciado por outro processo.
if (!parentPort) {
  throw new Error("Worker iniciado sem conexão com o processo principal.");
}

// Verifica se os dados foram recebidos corretamente.
if (!workerData || typeof workerData !== "object") {
  throw new Error("Dados da tarefa não foram recebidos.");
}

// Recebe os limites do intervalo enviados pelo processo principal.
const { inicio, fim } = workerData;

// Verifica se o intervalo recebido é válido.
if (
  !Number.isSafeInteger(inicio) ||
  !Number.isSafeInteger(fim) ||
  inicio < 1 ||
  fim < inicio ||
  fim > 1_000_000_000
) {
  throw new Error("Intervalo inválido.");
}

// ------------------------------------------------------------
// Execução do cálculo
// ------------------------------------------------------------

// Registra o momento em que o cálculo começa.
const start = performance.now();

// Variável que armazenará a soma das raízes quadradas.
let resultado = 0;

// Percorre o intervalo e soma a raiz quadrada de cada número.
for (let i = inicio; i <= fim; i++) {
  resultado += Math.sqrt(i);
}

// Calcula o tempo de execução do cálculo.
const kernelMs = performance.now() - start;

// Verifica se o resultado calculado é um número válido.
if (!Number.isFinite(resultado)) {
  throw new Error("O cálculo produziu um resultado numérico inválido.");
}

// ------------------------------------------------------------
// Envio do resultado ao processo principal
// ------------------------------------------------------------

// Envia o resultado e o tempo de execução para o cliente.
parentPort.postMessage({
  resultado,
  kernelMs,
});
