require_relative 'lib/concurrent-ruby-tcp'

TCP_PORT = 2_000

# shared functions
module RemoteFunctions
  def eval_pi(trial_count)
    @call_count += 1
    r = Random.new
    #Random failure
    raise "Computation error" if r.rand < 0.1
    # computation
    (4.0/trial_count) * trial_count.times.count do
      r.rand**2+r.rand**2 < 1
    end 
  end
end

# client class
class MyTCPClient < TCPClient
  attr_reader :call_count
  def initialize(addr, port)
    super(addr, port)
    @call_count = 0
  end
  include RemoteFunctions
end

def client_main(id)
  Log4r::NDC.push("client-#{id}")
  c = MyTCPClient.new('localhost', TCP_PORT).tap(&:run)
  LOGGER.info "exiting after #{c.call_count} call to eval_pi"
rescue => e
  LOGGER.fatal "Thread exception #{e}\n#{e.backtrace.join("\n")}"
ensure
  Log4r::NDC.pop
end

def start_client(id)
  LOGGER.info "starting client #{id}"
  Kernel.spawn(ENV, RbConfig.ruby, __FILE__, 'client', id.to_s, :out => :out, :err => :err)
end

def server_main
  Log4r::NDC.push('server')
  server = TCPWorkerPool.new(TCP_PORT)

  # eval_pi using several remote call to eval_pi
  task_count = 50
  trials_per_task = 1_000_000
  futures = task_count.times.map do
    server.future(:eval_pi, trials_per_task)
      .rescue{ |e| LOGGER.debug "Error detected: #{e}"; nil }
  end
  pi = Concurrent.zip(*futures).then do |*values|
    v = values.compact
    v.reduce(:+) / v.size
  end

  # start slowly slaves to check that tasks are shared
  children = 4.times.map {|i| start_client(i).tap{ sleep 2 } }
  LOGGER.debug "children: #{children}"

  puts "pi evaluated to #{pi.value!}"

  server.stop!
  children.each { |pid| Process.wait pid }
ensure
  Log4r::NDC.pop
end

if $0 == __FILE__
  LOGGER.level = Log4r::INFO
  if ARGV[0] == 'client'
    client_main(ARGV[1])
  else
    server_main
  end
end



