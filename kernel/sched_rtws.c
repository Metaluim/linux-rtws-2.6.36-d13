/*
 * Real-time Work-Stealing Scheduling Class (SCHED_RTWS)
 *
 * Global Earliest Deadline First (EDF) + Work-Stealing (WS).
 *
 *
 * Copyright (C) 2012 José Fonseca <jcnfonseca@gmail.com>,
 */

static const struct sched_class rtws_sched_class;

static inline u64 get_next_activation_period(struct sched_rtws_entity *rtws_se)
{
	return rtws_se->job.deadline + rtws_se->rtws_period - rtws_se->rtws_deadline;
}

/* Tells if value @a is lesser than value @b. */
static inline int time_before_rtws(u64 a, u64 b)
{
	return (s64)(a - b) < 0;
}

/* 
 * Tells if entity @a should preempt entity @b. That's true when:
 *
 *  - Entity @a is marked with SF_HEAD flag and entity @b not;
 *  - Entity @a has earlier deadline than entity @b and @b is not a 'special task'.
 */ 
static inline
int entity_preempt_rtws(struct sched_rtws_entity *a, struct sched_rtws_entity *b)
{
	
	return ((a->flags & SF_HEAD && !(b->flags & SF_HEAD)) ||
	       (!(b->flags & SF_HEAD) &&
		time_before_rtws(a->job.deadline, b->job.deadline)));
}

/* 
 * Tells if entity @a should preempt entity @b. That's true when:
 *
 *  - Above conditions are met;
 *  - Entity @a is marked with SF_HEAD flag;
 *  - Entity @a has equal deadline than entity @b and @b is not a 'special task'.
 */ 
static inline
int entity_preempt_equal_rtws(struct sched_rtws_entity *a, struct sched_rtws_entity *b)
{
	return ((a->job.deadline == b->job.deadline && !(b->flags & SF_HEAD)) ||
	        a->flags & SF_HEAD || entity_preempt_rtws(a, b));
}

static inline int on_global_rq(struct sched_rtws_entity *rtws_se)
{
	return !RB_EMPTY_NODE(&rtws_se->task_node);
}

static inline int on_rtws_rq(struct sched_rtws_entity *rtws_se)
{
	return !RB_EMPTY_NODE(&rtws_se->pjob_node);
}

static inline struct task_struct *task_of_rtws_se(struct sched_rtws_entity  *rtws_se)
{
	return container_of(rtws_se, struct task_struct, rtws);
}

static inline struct rq *rq_of_rtws_rq(struct rtws_rq *rtws_rq)
{
	return container_of(rtws_rq, struct rq, rtws);
}

static inline struct rtws_rq *rtws_rq_of_rtws_se(struct sched_rtws_entity *rtws_se)
{
	struct task_struct *p = task_of_rtws_se(rtws_se);
	struct rq *rq = task_rq(p);

	return &rq->rtws;
}

static inline int is_leftmost_pjob(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	return rtws_rq->leftmost_pjob == &rtws_se->pjob_node;
}

static inline int has_stealable_pjobs(struct rtws_rq *rtws_rq)
{
	return !RB_EMPTY_ROOT(&rtws_rq->stealable_pjobs);
}

static void enqueue_task_rtws(struct rq *rq, struct task_struct *p, int flags);
static void 
check_preempt_curr_rtws(struct rq *rq, struct task_struct *p, int flags);
static void __dequeue_task_rtws(struct global_rq *global_rq, struct sched_rtws_entity *rtws_se);
static void enqueue_pjob_rtws(struct rq *rq, struct task_struct *p, int flags);
static void update_task_rtws(struct rq *rq, struct sched_rtws_entity *rtws_se);
static int dispatch_rtws(struct rq *rq, struct task_struct *p);
static void __enqueue_task_rtws(struct global_rq *global_rq, struct sched_rtws_entity *rtws_se);
static int steal_pjob_rtws(struct rq *rq);

/*
 * We want the task to sleep while waiting for next job to become
 * ready, so we set the timer to the release instant and try to activate it.
 */
static void start_timer_rtws(struct rq *rq, struct sched_rtws_entity *rtws_se)
{
	ktime_t now, act;
	ktime_t soft, hard;
	unsigned long range;
	s64 delta;

	/*
	 * We want the timer to fire at given release time, but considering
	 * that it is actually coming from rq->clock and not from
	 * hrtimer's time base reading.
	 */
	act = ns_to_ktime(rtws_se->job.release);
	now = hrtimer_cb_get_time(&rtws_se->timer);
	delta = ktime_to_ns(now) - rq->clock;
	act = ktime_add_ns(act, delta);

	hrtimer_set_expires(&rtws_se->timer, act);

	soft = hrtimer_get_softexpires(&rtws_se->timer);
	hard = hrtimer_get_expires(&rtws_se->timer);
	//printk(KERN_INFO "expire: %Ld, soft: %Ld, hard: %Ld\n", ktime_to_ns(act), ktime_to_ns(soft), ktime_to_ns(hard));
	range = ktime_to_ns(ktime_sub(hard, soft));
	__hrtimer_start_range_ns(&rtws_se->timer, soft,
				 range, HRTIMER_MODE_ABS, 0);
}

/*
 * At this point we know the task is not on its rtws_rq because the timer was running, which
 * means the task has finished its last instance and was waiting for next activation period.
 */
static enum hrtimer_restart timer_rtws(struct hrtimer *timer)
{
	unsigned long flags;
	struct sched_rtws_entity *rtws_se = container_of(timer,
						     struct sched_rtws_entity,
						     timer);
	
	struct task_struct *p = task_of_rtws_se(rtws_se);
	struct rq *rq = task_rq_lock(p, &flags);

	//printk(KERN_INFO "**task %d state %ld on rq %d timer fired at time %Ld on cpu %d**\n", p->pid, p->state, p->se.on_rq, rq->clock, rq->cpu);

	/*
	 * We need to take care of a possible races here. In fact, the
	 * task might have changed its scheduling policy to something
	 * different from SCHED_RTWS (through sched_setscheduler()).
	 */
	if (!rtws_task(p))
		goto unlock;

	WARN_ON(rtws_se->parent);

	/* 
	 * To avoid contention on global rq and reduce some overhead,
	 * when a new task arrives and local rq is idle, we make sure
	 * it gets inserted on this rq. Otherwise we try to find a
	 * suitable rq for it. This way it only ends up in global rq
	 * when all rqs are busy and it has the lowest priority (latest
	 * deadline) comparing to running tasks.Nevertheless, since we
	 * are dealing with a new instance, scheduling parameters must be updated.
	 */
	update_task_rtws(rq, rtws_se);

	if (rq->rtws.nr_running && dispatch_rtws(rq, p))
		goto unlock;

	activate_task(rq, p, ENQUEUE_HEAD);
	resched_task(rq->curr);

unlock:
	task_rq_unlock(rq, &flags);

	return HRTIMER_NORESTART;
}

static void init_timer_rtws(struct sched_rtws_entity *rtws_se)
{
	struct hrtimer *timer = &rtws_se->timer;

	if (hrtimer_active(timer)) {
		hrtimer_try_to_cancel(timer);
		return;
	}

	hrtimer_init(timer, CLOCK_MONOTONIC, HRTIMER_MODE_REL);
	timer->function = timer_rtws;
}

static struct sched_rtws_entity *__pick_next_task_rtws(struct global_rq *global_rq)
{
	struct rb_node *edf = global_rq->leftmost_task;

	if (!edf)
		return NULL;

	return rb_entry(edf, struct sched_rtws_entity, task_node);
}

/*
 * Tells if selecting task @next would originate a priority inversion.
 * For that to happen task @next would have to have lower priority than
 * the highest priority task on global rq.
 * We don't care about idle or latest rq checking because the simple fact
 * of existing tasks on global rq guarantes correctness.
 */
static int 
priority_inversion_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *next)
{
	struct sched_rtws_entity *rtws_se;
	struct global_rq *global_rq = rtws_rq->global;
	int ret = 0;

	if (!global_rq->nr_running)
		return 0;

	rtws_se = __pick_next_task_rtws(global_rq);
	BUG_ON(!rtws_se);

	if (rtws_rq->nr_running && entity_preempt_rtws(rtws_se, next))
		ret = 1;

	return ret;
}

static void push_task_rtws(struct rq *rq, struct task_struct *p, int preempted)
{
	struct global_rq *global_rq = rq->rtws.global;

	if (preempted)
		deactivate_task(rq, p, 0);

	//printk(KERN_INFO "****global enqueue, task %d on cpu %d *******\n", p->pid, rq->cpu);

	raw_spin_lock(&global_rq->lock);
	__enqueue_task_rtws(global_rq, &p->rtws);
	raw_spin_unlock(&global_rq->lock);
}

static int pull_task_rtws(struct rq *this_rq)
{
	struct task_struct *p;
	struct sched_rtws_entity *rtws_se;
	struct global_rq *global_rq = this_rq->rtws.global;
	int ret = 0;

	if (!global_rq->nr_running)
		return 0;

	rtws_se = __pick_next_task_rtws(global_rq);

	if (unlikely(!rtws_se))
		return 0;

	p = task_of_rtws_se(rtws_se);
	WARN_ON(!rtws_task(p));

	//printk(KERN_INFO "= task %d stolen %d PULLED by cpu %d\n", p->pid, rtws_se->stolen, this_rq->cpu);
	
	/*
	 * We tranfer the task from global rq to this rq
	 */
	__dequeue_task_rtws(global_rq, rtws_se);
	
	if (rtws_se->stolen == this_rq->cpu)
		rtws_se->stolen = -1;

	set_task_cpu(p, this_rq->cpu);
	activate_task(this_rq, p, ENQUEUE_HEAD);

	ret = 1;
	this_rq->rtws.tot_pulls++;

	return ret;
}

static
void inc_pjobs_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	struct rq *rq = rq_of_rtws_rq(rtws_rq);
	struct task_struct *p = task_of_rtws_se(rtws_se);
	u64 deadline = rtws_se->job.deadline;

	WARN_ON(!rtws_prio(p->prio));
	rtws_rq->nr_running++;

	/* 
	 * We update statistics about this rq only when
	 * enqueueing pjob has lower deadline than our current
	 * one, if we got any running on, that's it.
	 */
	if (rtws_rq->earliest_dl == 0 || time_before_rtws(deadline, rtws_rq->earliest_dl)) {
		rtws_rq->earliest_dl = deadline;
		cpudl_set(&rq->rd->rtwsc_cpudl, rq->cpu, deadline, rtws_se->stolen, 1);
		smp_wmb();
	}
	//printk(KERN_INFO "enqueing task %d stolen %d on cpu %d, current deadline %llu, remaining tasks %lu\n", p->pid, rtws_se->stolen, rq->cpu, rtws_rq->earliest_dl, rtws_rq->nr_running);	
}

static
void dec_pjobs_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	struct rq *rq = rq_of_rtws_rq(rtws_rq);
	struct task_struct *p = task_of_rtws_se(rtws_se);
	struct sched_rtws_entity *stealable;
	struct global_rq * global_rq = rtws_rq->global;
	int ret = 0;

	WARN_ON(!rtws_prio(p->prio));
	WARN_ON(!rtws_rq->nr_running);
	rtws_rq->nr_running--;

	if (!rtws_rq->nr_running) {
		/* If there are no more pjobs to run, we declare this rq idle  */
		rtws_rq->earliest_dl = 0;
		cpudl_set(&rq->rd->rtwsc_cpudl, rq->cpu, 0, 0, 0);
		smp_wmb();
		raw_spin_lock(&global_rq->lock);
		ret = pull_task_rtws(rq);
		raw_spin_unlock(&global_rq->lock);

		if (!ret)
			ret = steal_pjob_rtws(rq);
	} else {
		if (!RB_EMPTY_NODE(&rtws_se->stealable_pjob_node))
			return;

		if (rtws_rq->earliest_dl != rtws_se->job.deadline)
			return;

		if (!has_stealable_pjobs(rtws_rq))
			return;

		/* The leftmost stealable pjob and our next pjob share the same deadline */
		stealable = rb_entry(rtws_rq->leftmost_stealable_pjob, struct sched_rtws_entity, stealable_pjob_node);

		if (stealable->job.deadline > rtws_rq->earliest_dl) {
			/*
			 * If the next pjob has lower priority than the highest priority task on
			 * global rq, we try to pull that task.
			 * Clearing the earlieast deadline value (earliest_dl=0) on the rq is
			 * a trick to allow next's rq statistics update, keeping the current ones.
			 */ 
			if (priority_inversion_rtws(rtws_rq, stealable)) {
				rtws_rq->earliest_dl = 0;

				raw_spin_lock(&global_rq->lock);
				ret = pull_task_rtws(rq);
				raw_spin_unlock(&global_rq->lock);

				if (ret)
					return;
			}	
		}

		/*
		 * We update statistics about this rq when next
		 * pjob has a different deadline than the dequeueing one.
		 */
		if (rtws_rq->earliest_dl != stealable->job.deadline) {
			rtws_rq->earliest_dl = stealable->job.deadline;
			cpudl_set(&rq->rd->rtwsc_cpudl, rq->cpu, rtws_rq->earliest_dl, 0, 1);
			smp_wmb();
		}
	}
	//printk(KERN_INFO "dequeing task %d on cpu %d, current deadline %llu, remaining tasks %lu\n", p->pid, rq->cpu, rtws_rq->earliest_dl, rtws_rq->nr_running);
}

/*
 * The list of ready -rtws pjobs is a rb-tree ordered by deadline.
 * Pjobs with equal deadline obey to a LIFO order.
 */
static void __enqueue_pjob_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	struct rb_node **link = &rtws_rq->pjobs.rb_node;
	struct rb_node *parent = NULL;
	struct sched_rtws_entity *entry;
	int edf = 1;

	BUG_ON(on_rtws_rq(rtws_se));

	/* TODO: Optimization in case enqueueing task is next edf */

	while (*link) {
		parent = *link;
		entry = rb_entry(parent, struct sched_rtws_entity, pjob_node);

		if ((rtws_se->help_first && !is_leftmost_pjob(rtws_rq, entry) && entity_preempt_equal_rtws(rtws_se, entry)) ||
		    (rtws_se->help_first && is_leftmost_pjob(rtws_rq, entry) && entity_preempt_rtws(rtws_se, entry)) ||
		    (!rtws_se->help_first && entity_preempt_equal_rtws(rtws_se, entry)))
			link = &parent->rb_left;
		else {
			link = &parent->rb_right;
			edf = 0;
		}
	}

	if (edf)
	 	rtws_rq->leftmost_pjob = &rtws_se->pjob_node;

	rb_link_node(&rtws_se->pjob_node, parent, link);
	rb_insert_color(&rtws_se->pjob_node, &rtws_rq->pjobs);

	rtws_rq->tot_enqueues++;
	inc_pjobs_rtws(rtws_rq, rtws_se);
}

/*
 * The list of stealable -rtws pjobs is a rb-tree with pjobs ordered by deadline,
 * excluding current pjob. Pjobs with equal deadline obey to a FIFO order.
 */
static void enqueue_stealable_pjob_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	struct rq *rq = rq_of_rtws_rq(rtws_rq);
	//struct task_struct *p = task_of_rtws_se(rtws_se);
	struct rb_node **link = &rtws_rq->stealable_pjobs.rb_node;
	struct rb_node *parent = NULL;
	struct sched_rtws_entity *entry;
	int edf = 1;

	BUG_ON(!RB_EMPTY_NODE(&rtws_se->stealable_pjob_node));

	//printk(KERN_INFO "****stealable pjob enqueue, task %d ****\n", p->pid);

	/* TODO: Optimization in case enqueueing task is next edf */

	while (*link) {
		parent = *link;
		entry = rb_entry(parent, struct sched_rtws_entity, stealable_pjob_node);

		if (entity_preempt_rtws(rtws_se, entry))
			link = &parent->rb_left;
		else {
			link = &parent->rb_right;
			edf = 0;
		}
	}

	if (edf) {
	 	rtws_rq->leftmost_stealable_pjob = &rtws_se->stealable_pjob_node;
		cpudl_set(&rq->rd->rtwss_cpudl, rq->cpu, rtws_se->job.deadline, 0, 1);
		smp_wmb();
	}
	
	rb_link_node(&rtws_se->stealable_pjob_node, parent, link);
	rb_insert_color(&rtws_se->stealable_pjob_node, &rtws_rq->stealable_pjobs);
}		

static void __dequeue_pjob_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	if (!on_rtws_rq(rtws_se))
		return;
		
	if (rtws_rq->leftmost_pjob == &rtws_se->pjob_node) {
		struct rb_node *next_node;

		next_node = rb_next(&rtws_se->pjob_node);
		rtws_rq->leftmost_pjob = next_node;
	}

	rb_erase(&rtws_se->pjob_node, &rtws_rq->pjobs);
	RB_CLEAR_NODE(&rtws_se->pjob_node);

	dec_pjobs_rtws(rtws_rq, rtws_se);
}

static void dequeue_stealable_pjob_rtws(struct rtws_rq *rtws_rq, struct sched_rtws_entity *rtws_se)
{
	//struct task_struct *p = task_of_rtws_se(rtws_se);
	struct rq *rq = rq_of_rtws_rq(rtws_rq);

	if (RB_EMPTY_NODE(&rtws_se->stealable_pjob_node))
		return;

	//printk(KERN_INFO "****stealable pjob dequeue, task %d ****\n", p->pid);
	
	if (rtws_rq->leftmost_stealable_pjob == &rtws_se->stealable_pjob_node) {
		struct rb_node *next_node;

		next_node = rb_next(&rtws_se->stealable_pjob_node);
		rtws_rq->leftmost_stealable_pjob = next_node;

		if (next_node)
			cpudl_set(&rq->rd->rtwss_cpudl, rq->cpu, rtws_se->job.deadline, 0, 1);
		else
			cpudl_set(&rq->rd->rtwss_cpudl, rq->cpu, 0, 0, 0);

		smp_wmb();
	}

	rb_erase(&rtws_se->stealable_pjob_node, &rtws_rq->stealable_pjobs);
	RB_CLEAR_NODE(&rtws_se->stealable_pjob_node);
}

static void enqueue_pjob_rtws(struct rq *rq, struct task_struct *p, int flags)
{
	struct sched_rtws_entity *rtws_se = &p->rtws;

	//printk(KERN_INFO "****pjob enqueue, task %d pjob %d****\n", p->pid, rtws_se->pjob);

	__enqueue_pjob_rtws(&rq->rtws, rtws_se);

	if (!task_current(rq, p) && rq->rtws.nr_running > 1 && !(flags & ENQUEUE_HEAD))
		enqueue_stealable_pjob_rtws(&rq->rtws, rtws_se);
}

static void dequeue_pjob_rtws(struct rq *rq, struct task_struct *p)
{
	struct sched_rtws_entity *rtws_se = &p->rtws;

	//printk(KERN_INFO "****pjob dequeue, task %d pjob %d****\n", p->pid, rtws_se->pjob);

	__dequeue_pjob_rtws(&rq->rtws, rtws_se);
	dequeue_stealable_pjob_rtws(&rq->rtws, rtws_se);
}

static struct sched_rtws_entity *pick_next_pjob_rtws(struct rtws_rq *rtws_rq)
{
	struct rb_node *edf = rtws_rq->leftmost_pjob;

	if (!edf)
		return NULL;

	return rb_entry(edf, struct sched_rtws_entity, pjob_node);
}

static struct task_struct *pick_next_stealable_pjob_rtws(struct rtws_rq *rtws_rq)
{
	struct sched_rtws_entity *rtws_se;
	struct task_struct *p;
	struct rq *rq = rq_of_rtws_rq(rtws_rq);

	if (!has_stealable_pjobs(rtws_rq))
		return NULL;

	rtws_se = rb_entry(rtws_rq->leftmost_stealable_pjob, struct sched_rtws_entity, stealable_pjob_node);
	BUG_ON(!rtws_se);
	
	p = task_of_rtws_se(rtws_se);
	BUG_ON(rq->cpu != task_cpu(p));
	BUG_ON(task_current(rq, p));

	BUG_ON(!p->se.on_rq);
	BUG_ON(!rtws_task(p));

	return p;
}

static int steal_pjob_rtws(struct rq *this_rq)
{
	int ret = 0, this_cpu = this_rq->cpu, target_cpu;
	struct task_struct *p;
	struct rq *target_rq;
	struct global_rq *global_rq = this_rq->rtws.global;

	if (global_rq->random) {
		/*
		 * Pseudo random selection of our victim rq,
		 * among rqs with to-be-stolen pjobs, that's it.
		 */	
		target_cpu = find_random_stealable_cpu_rtws(&this_rq->rd->rtwss_cpudl, this_rq->cpu);
	} else {
		/*
		 * When not in random mode, we gotta find the rq with the earliest
		 * deadline stealable pjob.
		 */
		target_cpu = find_earliest_stealable_cpu_rtws(&this_rq->rd->rtwss_cpudl);
	}

	if (target_cpu == -1)
		return 0;

	//printk(KERN_INFO "stealable cpu %d\n", target_cpu);

	target_rq = cpu_rq(target_cpu);

	/*
	 * We can potentially drop this_rq's lock in
	 * double_lock_balance, and another CPU could alter this_rq
	 */
	double_lock_balance(this_rq, target_rq);
	
	if (unlikely(target_rq->rtws.nr_running <= 1))
		goto unlock;

	if (unlikely(this_rq->rtws.nr_running))
		goto unlock;

	p = pick_next_stealable_pjob_rtws(&target_rq->rtws);

	if (p) {
		if (p->rtws.nr_pjobs > 0)
			goto unlock;

		WARN_ON(p == target_rq->curr);
		WARN_ON(!p->se.on_rq);
		WARN_ON(!rtws_task(p));

		deactivate_task(target_rq, p, 0);

		p->rtws.stolen = target_cpu;

		set_task_cpu(p, this_cpu);
		activate_task(this_rq, p, 0);

		this_rq->rtws.tot_steals++;
		//printk(KERN_INFO "=task %d STOLEN by cpu %d from cpu %d!\n", p->pid, this_cpu, target_cpu);
		ret = 1;
	}

unlock:
	double_unlock_balance(this_rq, target_rq);

	return ret;
}

/* 
 * Tries to push a -rtws task to a "random" idle rq.
 */
static int push_idle_rtws(struct rq *this_rq, struct task_struct *p)
{
	struct rq *target_rq;
	int ret = 0, target_cpu;
	struct cpudl *cp = &this_rq->rd->rtwsc_cpudl;

retry:	
	target_cpu = find_idle_cpu_rtws(cp);

	if (target_cpu == -1)
		return 0;

	//printk(KERN_INFO "idle cpu %d\n", target_cpu);	

	target_rq = cpu_rq(target_cpu);

	/* We might release rq lock */
	get_task_struct(p);
			
	double_lock_balance(this_rq, target_rq);

	if (unlikely(target_rq->rtws.nr_running)) {
		double_unlock_balance(this_rq, target_rq);
		put_task_struct(p);
		target_rq = NULL;
		goto retry;
	}

	if (p->rtws.parent) {
		deactivate_task(this_rq, p, 0);
		if (p->state != TASK_WAKING)
			p->rtws.stolen = this_rq->cpu;
	}

	set_task_cpu(p, target_cpu);	
	activate_task(target_rq, p, 0);

	ret = 1;
	resched_task(target_rq->curr);

	double_unlock_balance(this_rq, target_rq);	
	put_task_struct(p);

	return ret;
}

/* 
 * Pushes a -rtws task to the latest rq if its currently executing task has lower priority.
 * Only remote rqs are consider here.
 */
static int push_latest_rtws(struct rq *this_rq, struct task_struct *p, int target_cpu)
{
	struct rq *target_rq;
	int ret = 0;

	target_rq = cpu_rq(target_cpu);

	/* We might release rq lock */
	get_task_struct(p);

	//printk(KERN_INFO "check preempting other %llu - %llu\n", p->rtws.job.deadline, target_rq->rtws.earliest_dl);	
		
	double_lock_balance(this_rq, target_rq);

	/* TODO check if in the meanwhile a task was dispatched to here */
	if (target_rq->rtws.nr_running && !time_before_rtws(p->rtws.job.deadline, target_rq->rtws.earliest_dl))
		goto unlock;
	
	set_task_cpu(p, target_cpu);
	activate_task(target_rq, p, ENQUEUE_HEAD);
	ret = 1;

	resched_task(target_rq->curr);

unlock:
	double_unlock_balance(this_rq, target_rq);
	put_task_struct(p);

	return ret;
}

static int dispatch_rtws(struct rq *rq, struct task_struct *p)
{
	struct sched_rtws_entity *edf;
	int target_cpu;
	struct cpudl *cp = &rq->rd->rtwsc_cpudl;

	edf = __pick_next_task_rtws(rq->rtws.global);

	if (edf && !time_before_rtws(p->rtws.job.deadline, edf->job.deadline))
		goto global;

	if (push_idle_rtws(rq, p))
		return 1;

	target_cpu = find_latest_cpu_rtws(cp);

	//printk(KERN_INFO "latest cpu %d\n", target_cpu);

	if (target_cpu == rq->cpu) {
		//printk(KERN_INFO "check preempting itself %llu - %llu\n", p->rtws.job.deadline, rq->rtws.earliest_dl);
		if (rq->rtws.nr_running && !time_before_rtws(p->rtws.job.deadline, rq->rtws.earliest_dl))
			goto global;

		return 0;
	}

	if (push_latest_rtws(rq, p, target_cpu))
		return 1;

global:
	push_task_rtws(rq, p, 0);
	return 1;
}

/******************************************************************/
/***************************** rtws SCHEDULING POLICY ************/

/*
 * When a -rtws task is queued back on the global runqueue, its deadline need updating.
 *
 * We are being explicitly informed that a new instance is starting,
 * and this means that:
 *  - the absolute deadline of the task has to be placed at
 *    current time + relative deadline;
 *  - the runtime of the task has to be set to the maximum value.
 *
 * Pure Earliest Deadline First (EDF) scheduling does not deal with the
 * possibility of a entity lasting more than what it declared, and thus
 * exhausting its runtime.
 */
static inline void update_task_rtws(struct rq *rq, struct sched_rtws_entity *rtws_se)
{
	//struct task_struct *p = task_of_rtws_se(rtws_se);
	
	rtws_se->stolen = -1;
	rtws_se->nr_pjobs = 0;
	rtws_se->throttled = 0;
	atomic_inc(&rtws_se->job.nr);
		
	rtws_se->job.deadline = rq->clock + rtws_se->rtws_deadline;
	//printk(KERN_INFO "****update task, task %d new dl %Ld****\n", p->pid, rtws_se->job.deadline);
}

static inline
int deadline_missed_rtws(struct rq *rq, struct sched_rtws_entity *rtws_se)
{
	int dmiss;
	u64 damount;

	/* Pjobs are ignored because they share root task's deadline */
	if (rtws_se->parent)
		return 0;

	//printk(KERN_INFO "****dl miss, dl %Lu clock %Lu****\n", rtws_se->job.deadline, rq->clock);

	dmiss = time_before_rtws(rtws_se->job.release, rq->clock);

	if (!dmiss)
		return 0;

	/*
	 * Record statistics about maximum deadline
	 * misses.
	 */
	damount = rq->clock - rtws_se->job.release;

	//rtws_se->stats.dmiss_max = max(rtws_se->stats.dmiss_max, damount);
	rtws_se->stats.dmiss_max = 1;
	
	return 1;
}

/*
 * Update the current task's statistics (provided it is still
 * a -rtws task and has not been removed from the rtws_rq).
 */
static void update_curr_rtws(struct rq *rq)
{
	struct task_struct *curr = rq->curr;
	struct sched_rtws_entity *rtws_se = &curr->rtws;
	u64 delta_exec;

	if (!task_has_rtws_policy(curr))
		return;

	delta_exec = rq->clock - curr->se.exec_start;
	if (unlikely((s64)delta_exec < 0))
		delta_exec = 0;

	schedstat_set(curr->se.statistics.exec_max,
		      max(curr->se.statistics.exec_max, delta_exec));

	curr->se.sum_exec_runtime += delta_exec;
	schedstat_add(&rq->rtws, exec_clock, delta_exec);
	/* Maintain exec runtime for a thread group. @sched_stats.h */
	account_group_exec_runtime(curr, delta_exec);

	curr->se.exec_start = rq->clock;
	/* Charge this task's execution time to its accounting group. @sched.c */
	cpuacct_charge(curr, delta_exec);
	sched_rt_avg_update(rq, delta_exec);

	rtws_se->stats.tot_runtime += delta_exec;
}

static void __enqueue_task_rtws(struct global_rq *global_rq, struct sched_rtws_entity *rtws_se)
{	
	struct rb_node **link = &global_rq->tasks.rb_node;
	struct rb_node *parent = NULL;
	struct sched_rtws_entity *entry;
	int edf = 1;

	BUG_ON(on_global_rq(rtws_se));

	/* TODO: Optimization in case enqueueing task is next edf */

	while (*link) {
		parent = *link;
		entry = rb_entry(parent, struct sched_rtws_entity, task_node);

		if (entity_preempt_rtws(rtws_se, entry))
			link = &parent->rb_left;
		else {
			link = &parent->rb_right;
			edf = 0;
		}
	}

	if (edf)
	 	global_rq->leftmost_task = &rtws_se->task_node;
	
	rb_link_node(&rtws_se->task_node, parent, link);
	rb_insert_color(&rtws_se->task_node, &global_rq->tasks);

	global_rq->nr_running++;
}

static void __dequeue_task_rtws(struct global_rq *global_rq, struct sched_rtws_entity *rtws_se)
{
	if (!on_global_rq(rtws_se))
		return;

	global_rq->nr_running--;

	if (global_rq->leftmost_task == &rtws_se->task_node) {
		struct rb_node *next_node;

		next_node = rb_next(&rtws_se->task_node);
		global_rq->leftmost_task = next_node;
	}

	rb_erase(&rtws_se->task_node, &global_rq->tasks);
	RB_CLEAR_NODE(&rtws_se->task_node);
}

static void
enqueue_task_rtws(struct rq *rq, struct task_struct *p, int flags)
{
	struct sched_rtws_entity *rtws_se = &p->rtws;

	/*
	 * If p is throttled, we do nothing. In fact, to admitted back this task,
	 * its timer has to fire or someone has to call sched_setscheduler() on it.
	 */
	if (rtws_se->throttled)
		return;

	//printk(KERN_INFO "***original enqueue, running %lu task %d pjobs %lu state %lu on cpu %d***\n", rq->rtws.nr_running, p->pid, p->rtws.nr_pjobs, p->state, rq->cpu);

	if (p->state == TASK_RUNNING && rtws_se->parent && rq->rtws.nr_running)
		if (push_idle_rtws(rq, p))
			return;

	if (p->state == TASK_WAKING && rq->rtws.nr_running) {
		if (time_before_rtws(rtws_se->job.deadline, rq->rtws.earliest_dl))
			flags = ENQUEUE_HEAD;

		if ((!rtws_se->parent && rtws_se->nr_pjobs == 0) ||
		    (rtws_se->stolen != -1 && !time_before_rtws(rtws_se->job.deadline, rq->rtws.earliest_dl)))
			if (dispatch_rtws(rq, p))
				return;
	}

	enqueue_pjob_rtws(rq, p, flags);	
}

static void
dequeue_task_rtws(struct rq *rq, struct task_struct *p, int sleep)
{ 
	//printk(KERN_INFO "********original dequeue, task %d state %lu on cpu %d*******\n", p->pid, p->state, rq->cpu);

	dequeue_pjob_rtws(rq, p);
}

/*
 * This function makes the task sleep until at least the absolute time
 * instant specified in @rqtp.
 * In fact, since we want to wake up the task with its full runtime,
 * @rqtp might be too early (or the task might already have overrun
 * its runtime when calling this), the sleeping time may be longer
 * than asked.
 *
 * This is intended to be used at the end of a periodic -rtws task
 * instance.
 */
static long wait_interval_rtws(struct task_struct *p, struct timespec *rqtp,
			     struct timespec *rmtp)
{
	unsigned long flags;
	struct sched_rtws_entity *rtws_se = &p->rtws;
	struct rq *rq = task_rq_lock(p, &flags);
	u64 wakeup;
	long ret = 0;

	/* Only "root" tasks have periodic behaviour. */
	if (unlikely(rtws_se->parent)) {
		ret = -EINVAL;
		goto unlock;
	}

	/*
	 * If no wakeup time is provided, sleep at least up to the
	 * next activation period.
	 */
	if (!rqtp) {
		wakeup = get_next_activation_period(rtws_se);
		goto activate;
	}

	/* We allow tasks to wake up before or after its next activation period */
	wakeup = timespec_to_ns(rqtp);

	//printk(KERN_INFO "--wake up %llu rq clock %llu, task %d on cpu %d--\n", wakeup, rq->clock, p->pid, rq->cpu);

activate:
	rtws_se->job.release = wakeup;

	if (deadline_missed_rtws(rq, rtws_se)) {
		printk(KERN_INFO "missed deadline\n");
		deactivate_task(rq, p, 0);
		update_task_rtws(rq, rtws_se);
		activate_task(rq, p, 0);
		ret = 1;
	} else {
		deactivate_task(rq, p, 0);
		rtws_se->throttled = 1;
		start_timer_rtws(rq, rtws_se);
		ret = 0;
	}

	resched_task(rq->curr);
unlock:
	task_rq_unlock(rq, &flags);

	return ret;
}

/*
 * Only called when both the current and p are -rtws
 * tasks.
 */
static void 
check_preempt_curr_rtws(struct rq *rq, struct task_struct *p, int flags)
{
	//printk("********check preempt curr, task %d on cpu %d*******\n",p->pid, rq->cpu);
	/*if (dl_task(p)) {
		resched_task(rq->curr);
		return;
	}*/

	if (!is_leftmost_pjob(&rq->rtws, &rq->curr->rtws))
		resched_task(rq->curr);
}

static struct task_struct *
pick_next_task_rtws(struct rq *rq)
{
	struct sched_rtws_entity *rtws_se;
	struct task_struct *next;
	struct rtws_rq *rtws_rq = &rq->rtws;
	//struct global_rq *global_rq = rtws_rq->global;
	//int ret;

	/* TODO: if new pjobs spawn after we get a null here, we wont be aware of them until next pick round */
	if (!rtws_rq->nr_running) {
		/*raw_spin_lock(&global_rq->lock);
		ret = pull_task_rtws(rq);
		raw_spin_unlock(&global_rq->lock);

		if (!ret)*/
			//if (!steal_pjob_rtws(rq))
		return NULL;
	}

	rtws_se = pick_next_pjob_rtws(rtws_rq);
	BUG_ON(!rtws_se);

	next = task_of_rtws_se(rtws_se);
	next->se.exec_start = rq->clock;

	//printk(KERN_INFO "picking task %d on rq %d....\n", next->pid, rq->cpu);
	/* Running task will never be stolen. */
	dequeue_stealable_pjob_rtws(rtws_rq, rtws_se);

	rtws_rq->tot_picks++;
	
	return next;
}

static void 
put_prev_task_rtws(struct rq *rq, struct task_struct *prev)
{
	struct sched_rtws_entity *rtws_se = &prev->rtws;
	int migrate = 0;

	//printk(KERN_INFO "********put prev task, task %d on cpu %d stolen %d running %lu*******\n",prev->pid, rq->cpu, rtws_se->stolen, rq->rtws.nr_running);
	
	update_curr_rtws(rq);
	prev->se.exec_start = 0;

	/* A stolen pjob can not be delayed by a preemption like any other local pjob,
	 * because that blocking time could make it lose its deadline. That is, instead of helping
	 * it to finish earlier, this rq would add a substancial response time, which could lead
	 * in a execution time higher than WCET (sequential-based): a deadline violation certainly
	 * avoidable, if the pjob hasn't been stolen.
	 * By forcing pjob=0, we make sure this pjob ends up on global rq, thus becoming able
	 * to preempt other cores or to be selected on the 2nd highest priority picking up phase.
	 * */
	
	if (rtws_se->stolen != -1)
		migrate = 1;

	if (on_rtws_rq(rtws_se) && rq->rtws.nr_running > 1) {
		if ((!rtws_se->parent && rtws_se->nr_pjobs == 0) || migrate)
			push_task_rtws(rq, prev, 1);
		else
			enqueue_stealable_pjob_rtws(&rq->rtws, rtws_se);
	}
}

#ifdef CONFIG_SMP

static int select_task_rq_rtws(struct rq *rq, struct task_struct *p, int sd_flag, int flags)
{
	//printk("********select task rq, task %d of cpu %d on cpu %d*******\n",p->pid, task_cpu(p), smp_processor_id());
	return task_cpu(p);
}

#endif /* CONFIG_SMP */

static void 
set_curr_task_rtws(struct rq *rq)
{
	struct task_struct *p = rq->curr;
	//printk(KERN_INFO "********set curr task, task %d on cpu %d*******\n", rq->curr->pid, rq->cpu);

	update_task_rtws(rq, &p->rtws);
	p->se.exec_start = rq->clock;
}

static void 
task_tick_rtws(struct rq *rq, struct task_struct *curr, int queued)
{
	update_curr_rtws(rq);
}

static void task_fork_rtws(struct task_struct *p)
{
	struct sched_rtws_entity *rtws_se = &p->rtws;

	//printk(KERN_INFO "********task fork, task %d parent %d *******\n", p->pid, rtws_se->parent ? 1 : 0);

	if (!rtws_se->parent) {
		/*
	 	 * The task child of a -rtws task will remain SCHED_RTWS but
	 	 * throttled. This means the parent (or someone else)
	 	 * must call sched_setscheduler_ex() on it, or it won't even
	 	 * start.
	 	 */

		/* DOUBT: can't we just cancel timers for both pjobs and task,
		 * once init_timer_rtws() will be called on new tasks? */
		hrtimer_try_to_cancel(&rtws_se->timer);
		//hrtimer_init(&rtws_se->timer, CLOCK_MONOTONIC, HRTIMER_MODE_REL);
		rtws_se->rtws_deadline = rtws_se->job.deadline = 0;
		rtws_se->rtws_period = 0;
		rtws_se->throttled = 1;
	} else 
		hrtimer_try_to_cancel(&rtws_se->timer);	
}

static void task_dead_rtws(struct task_struct *p)
{
	struct sched_rtws_entity *rtws_se = &p->rtws;
	struct rq *rq = task_rq(p);
	struct global_rq *global_rq = rq->rtws.global;

	//printk(KERN_INFO "********task dead, task %d parent %d on cpu %d clock %Ld*******\n", p->pid, rtws_se->parent ? 1 : 0, rq->cpu, rq->clock);

	/* Timers are used only by root tasks */
	if (!rtws_se->parent) {
		if (rtws_se->stats.dmiss_max > 0) {
			if (global_rq->random)
				trace_sched_task_stat_rtws_random(p, rq->clock);
			else
				trace_sched_task_stat_rtws_earliest(p, rq->clock);
		}
		hrtimer_try_to_cancel(&rtws_se->timer);
	}
}

static void
switched_from_rtws(struct rq *rq, struct task_struct *p, int running)
{
	//printk(KERN_INFO "********switch from, task %d on cpu %d*******\n", rq->curr->pid, rq->cpu);

	if (hrtimer_active(&p->rtws.timer) && !rtws_policy(p->policy))
		hrtimer_try_to_cancel(&p->rtws.timer);	
}

static void 
switched_to_rtws(struct rq *rq, struct task_struct *p, int running)
{
	//printk(KERN_INFO "********switch to, task %d running %d on cpu %d*******\n",p->pid, running, rq->cpu);

	/*
	 * If p is throttled, don't consider the possibility
	 * of preempting rq->curr.
	 */
	if (unlikely(p->rtws.throttled))
		return;

	if (!running)
		check_preempt_curr_rtws(rq, p, 0);
}

/*
 * Only "root" tasks with no clildren are allowed to change
 * scheduling parameters! Anything else will be removed from rq
 * and throttled.
 *
 * If the scheduling parameters of a -rtws task changed,
 * a resched might be needed.
 */
static void 
prio_changed_rtws(struct rq *rq, struct task_struct *p,
			      int oldprio, int running)
{
	//rq != task_rq(p)
	/*struct sched_rtws_entity *rtws_se = &p->rtws;

	printk(KERN_INFO "********prio changed, task %d pjob %d running %d on cpu %d*******\n",p->pid, rtws_se->pjob, running, rq->cpu);
	
	if (rtws_se->parent || rtws_se->nr_pjobs > 0) {
		printk(KERN_INFO "problems...\n");
		WARN_ON(1);
		dequeue_task_rtws(rq, p, 0);
		rtws_se->throttled = 1;
		return;
	}
		
	if (running)*/
		/*
		 * We don't know if p has a earlier
		 * or later deadline, so let's blindly set a
		 * (maybe not needed) rescheduling point.
		 */
		/*resched_task(p);
	else
		switched_to_rtws(rq, p, running);*/
}

static const struct sched_class rtws_sched_class = {
	.next			= &rt_sched_class,

	.enqueue_task		= enqueue_task_rtws,
	.dequeue_task		= dequeue_task_rtws,
	//.yield_task		= yield_task_rtws,
	.wait_interval		= wait_interval_rtws,

	.check_preempt_curr	= check_preempt_curr_rtws,

	.pick_next_task		= pick_next_task_rtws,
	.put_prev_task		= put_prev_task_rtws,

#ifdef CONFIG_SMP
	.select_task_rq		= select_task_rq_rtws,

	/*.pre_schedule		= pre_schedule_rtws,
	.post_schedule	= post_schedule_rtws,
	.task_waking		= task_waking_rtws,
	.task_woken		= task_woken_rtws,

	.set_cpus_allowed       = set_cpus_allowed_rtws,

	.rq_online              = rq_online_rtws,
	.rq_offline             = rq_offline_rtws,*/
#endif

	.set_curr_task          = set_curr_task_rtws,
	.task_tick		= task_tick_rtws,
	.task_fork		= task_fork_rtws,
	.task_dead		= task_dead_rtws,

	.switched_from		= switched_from_rtws,
	.switched_to		= switched_to_rtws,
	.prio_changed		= prio_changed_rtws

	//.get_rr_interval	= get_rr_interval_rtws
};

/******************************************************************/
/***************************** INIT RQS  ********************************/
static void init_global_rq(struct global_rq *global_rq)
{
	printk(KERN_INFO "SCHED_RTWS: init_global_rq\n");

	raw_spin_lock_init(&global_rq->lock);
	global_rq->tasks = RB_ROOT;
	global_rq->nr_running = 0;
	global_rq->random = 0;
}

static void init_rtws_rq(struct rtws_rq *rtws_rq, struct rq *rq, struct global_rq *global_rq)
{
	printk(KERN_INFO "SCHED_RTWS: init_rtws_rq\n");
	
	rtws_rq->global = global_rq;
	rtws_rq->pjobs = RB_ROOT;
	rtws_rq->stealable_pjobs = RB_ROOT;
	rtws_rq->nr_running = 0;
	rtws_rq->tot_steals = 0;
	rtws_rq->tot_enqueues = 0;
	rtws_rq->tot_picks = 0;
	rtws_rq->tot_pulls = 0;
	rtws_rq->tot_cs = 0;
	rtws_rq->earliest_dl = 0;
}

#ifdef CONFIG_SCHED_DEBUG
extern void proc_sched_show_task(struct task_struct *p, struct seq_file *m);
extern void print_rtws_rq(struct seq_file *m, int cpu, struct rtws_rq *rtws_rq);

static void print_rtws_stats(struct seq_file *m, int cpu)
{
	struct rtws_rq *rtws_rq;	

	rtws_rq = &cpu_rq(cpu)->rtws;
	rcu_read_lock();
	print_rtws_rq(m, cpu, rtws_rq);
	rcu_read_unlock();
}
#endif
